#include "rdma_skiplist.h"

#include "macro.h"
#include "random.h"

namespace SKIPLIST {

Server::Server(Config& config) : dev("mlx5_0", 1, config.roce_flag), ser(dev) {
  seg_mr = dev.reg_mr(233, config.mem_size);
  alloc.Set((char*)seg_mr->addr, seg_mr->length);
  log_err("%lu~%lu\n", (uintptr_t)seg_mr->addr,
          (uintptr_t)seg_mr->addr + seg_mr->length);
  Init();
  // log_err("init");
  ser.start_serve();
}

void Server::Init() {
  char* raw =
      alloc.alloc(2 * sizeof(int64_t) + sizeof(uintptr_t) * kMaxHeight_);
  memset(raw, 0, 2 * sizeof(int64_t) + sizeof(uintptr_t) * kMaxHeight_);
  *reinterpret_cast<uint64_t*>(raw) = 1;
}

Server::~Server() { rdma_free_mr(seg_mr); }

Client::Client(Config& config, ibv_mr* _lmr, rdma_client* _cli,
               rdma_conn* _conn, rdma_conn* _wowait_conn, uint64_t _machine_id,
               uint64_t _cli_id, uint64_t _coro_id) {
  // id info
  machine_id = _machine_id;
  cli_id = _cli_id;
  coro_id = _coro_id;

  // rdma utils
  cli = _cli;
  conn = _conn;
  wo_wait_conn = _wowait_conn;
  lmr = _lmr;

  perf.init();
  sum_cost.init();

  // alloc info
  alloc.Set((char*)lmr->addr, lmr->length);
  seg_rmr = cli->run(conn->query_remote_mr(233));
  uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 10)) /
                       (config.num_machine * config.num_cli *
                        config.num_coro);  // 头部保留5GB，其他的留给client
  uint64_t buf_id = config.machine_id * config.num_cli * config.num_coro +
                    cli_id * config.num_coro + coro_id;
  uintptr_t remote_ptr =
      seg_rmr.raddr + seg_rmr.rlen - rbuf_size * buf_id;  // 从尾部开始分配
  ralloc.SetRemote(remote_ptr, rbuf_size, seg_rmr.raddr, seg_rmr.rlen);
  ralloc.alloc(ALIGNED_SIZE);  // 提前分配ALIGNED_SIZE，免得读取的时候越界
  log_err("%lu~%lu\n", (uintptr_t)lmr->addr,
          (uintptr_t)lmr->addr + lmr->length);
  // log_err("ralloc start_addr:%lx offset_max:%lx", ralloc.raddr,
  // ralloc.rsize);

  // util variable
  op_cnt = 0;
  miss_cnt = 0;
}

Client::~Client() {
  log_err("[%lu:%lu] miss_cnt:%lu", cli_id, coro_id, miss_cnt);
  perf.print(cli_id, coro_id);
  sum_cost.print(cli_id, coro_id);
}

task<> Client::start(uint64_t total) {
  uint64_t* start_cnt = (uint64_t*)alloc.alloc(sizeof(uint64_t), true);
  *start_cnt = 0;
  co_await conn->fetch_add(
      seg_rmr.raddr + sizeof(uintptr_t) * kMaxHeight_ + sizeof(uint64_t),
      seg_rmr.rkey, *start_cnt, 1);
  // log_info("Start_cnt:%lu", *start_cnt);
  while ((*start_cnt) < total) {
    // log_info("Start_cnt:%lu", *start_cnt);
    co_await conn->read(
        seg_rmr.raddr + sizeof(uintptr_t) * kMaxHeight_ + sizeof(uint64_t),
        seg_rmr.rkey, start_cnt, sizeof(uint64_t), lmr->lkey);
  }
}

task<> Client::stop() {
  uint64_t* start_cnt = (uint64_t*)alloc.alloc(sizeof(uint64_t));
  co_await conn->fetch_add(
      seg_rmr.raddr + sizeof(uintptr_t) * kMaxHeight_ + sizeof(uint64_t),
      seg_rmr.rkey, *start_cnt, -1);
  // log_err("Start_cnt:%lu", *start_cnt);
  while ((*start_cnt) != 0) {
    // log_info("Start_cnt:%lu", *start_cnt);
    co_await conn->read(
        seg_rmr.raddr + sizeof(uintptr_t) * kMaxHeight_ + sizeof(uint64_t),
        seg_rmr.rkey, start_cnt, sizeof(uint64_t), lmr->lkey);
  }
}

void Client::Node::StashHeight(const uint64_t height) {
  memcpy(static_cast<void*>(&next_[0]), &height, sizeof(height));
}

uint64_t Client::Node::UnstashHeight() const {
  uint64_t res;
  memcpy(&res, &next_[0], sizeof(res));
  return res;
}

uintptr_t Client::GetHead() {
  return seg_rmr.raddr + sizeof(uint64_t) +
         sizeof(uintptr_t) * (kMaxHeight_ - 1);
}

int64_t* Client::Node::Key() { return reinterpret_cast<int64_t*>(&next_[1]); }

int64_t* Client::Node::Value() {
  return reinterpret_cast<int64_t*>(&next_[1] + 1);
}

task<int64_t> Client::NodeKey(uintptr_t node) {
  char* raw = alloc.alloc(sizeof(int64_t));
  co_await cli->read(node + sizeof(Node), reinterpret_cast<void*>(raw),
                     sizeof(int64_t), lmr->lkey);
  co_return *reinterpret_cast<int64_t*>(raw);
}

task<int64_t> Client::NodeValue(uintptr_t node) {
  char* raw = alloc.alloc(sizeof(int64_t));
  co_await cli->read(node + sizeof(Node) + sizeof(int64_t),
                     reinterpret_cast<void*>(raw), sizeof(int64_t), lmr->lkey);
  co_return *reinterpret_cast<int64_t*>(raw);
}

task<uintptr_t> Client::NodeNext(uintptr_t node, int n) {
  uintptr_t next = node - sizeof(uintptr_t) * n;
  char* raw = alloc.alloc(sizeof(uintptr_t));
  co_await cli->read(next, reinterpret_cast<void*>(raw), sizeof(uintptr_t),
                     lmr->lkey);
  co_return *reinterpret_cast<uintptr_t*>(raw);
}

task<> Client::NodeSetNext(uintptr_t node, int n, uintptr_t x) {
  uintptr_t* raw = reinterpret_cast<uintptr_t*>(alloc.alloc(sizeof(uintptr_t)));
  *raw = x;
  co_await cli->write(node - sizeof(uintptr_t) * n, raw, sizeof(uintptr_t),
                      lmr->lkey);
  co_return;
}

task<bool> Client::NodeCASNext(uintptr_t node, int n, uintptr_t expected,
                               uintptr_t x) {
  uintptr_t* raw = reinterpret_cast<uintptr_t*>(alloc.alloc(sizeof(uintptr_t)));
  *raw = expected;
  co_return co_await cli->cas(node - sizeof(uintptr_t) * n, *raw, x);
}

task<uint64_t*> Client::GetMaxHeight(uintptr_t raddr) {
  void* raw = alloc.alloc(sizeof(uint64_t));
  co_await cli->read(raddr, raw, sizeof(uint64_t), lmr->lkey);
  co_return reinterpret_cast<uint64_t*>(raw);
}

int Client::RandomHeight() {
  int height = 1;
  while (height < kMaxHeight_ &&
         MySkipList::rand_() < kScaledInverseBranching_) {
    ++height;
  }
  return height;
}

Client::Node* Client::AllocateKeyAndValue(const int64_t key,
                                          const int64_t value) {
  return AllocateNode(key, value, RandomHeight());
}

Client::Node* Client::AllocateNode(const int64_t key, const int64_t value,
                                   const int height) {
  uint64_t pre = sizeof(uintptr_t) * (height - 1);
  char* st = alloc.alloc(pre + sizeof(Node) + sizeof(key) + sizeof(value));
  Node* x = reinterpret_cast<Node*>(st + pre);
  x->StashHeight(height);
  memcpy(st + pre + sizeof(Node), &key, sizeof(key));
  memcpy(st + pre + sizeof(Node) + sizeof(key), &value, sizeof(value));
  return x;
}

Client::Splice* Client::AllocateSpliceOnHeap() {
  size_t array_siz = sizeof(uintptr_t) * (kMaxHeight_ + 1);
  char* raw = alloc.alloc(sizeof(Splice) + 2 * array_siz);
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<uintptr_t*>(raw + sizeof(Splice));
  splice->next_ =
      reinterpret_cast<uintptr_t*>(raw + sizeof(Splice) + array_siz);
  return splice;
}

bool Client::Equal(const int64_t& a, const int64_t& b) const { return a == b; }

bool Client::LessThan(const int64_t& a, const int64_t& b) const {
  return a < b;
}

task<bool> Client::KeyIsAfterNode(const int64_t key, uintptr_t node) {
  co_return node != 0 && LessThan(co_await NodeKey(node), key);
}

task<bool> Client::KeyIsBeforeNode(const int64_t key, uintptr_t node) {
  co_return node == 0 || LessThan(key, co_await NodeKey(node));
}

task<bool> Client::InsertWithHintConcurrently(Node* x, void** hint) {
  Splice* splice = reinterpret_cast<Splice*>(*hint);
  if (splice == nullptr) {
    splice = AllocateSpliceOnHeap();
    *hint = reinterpret_cast<void*>(splice);
  }
  co_return co_await Insert(x, splice, true);
}

task<bool> Client::InsertConcurrently(Node* x) {
  Splice* splice = AllocateSpliceOnHeap();
  bool bo = co_await Insert(x, splice, false);
  co_return bo;
}

task<bool> Client::Insert(int64_t key, int64_t value) {
  alloc.ReSet(0);
  co_return co_await InsertConcurrently(AllocateKeyAndValue(key, value));
}

task<> Client::FindSpliceForLevel(const int64_t key, uintptr_t before,
                                  uintptr_t after, int level,
                                  uintptr_t* out_prev, uintptr_t* out_next) {
  while (true) {
    uintptr_t next = co_await NodeNext(before, level);
    if (next == after || !co_await KeyIsAfterNode(key, next)) {
      *out_prev = before;
      *out_next = next;
      co_return;
    }
    before = next;
  }
}

task<> Client::RecomputeSpliceLevels(const int64_t key, Splice* splice,
                                     int recompute_level) {
  for (int i = recompute_level - 1; i >= 0; --i) {
    co_await FindSpliceForLevel(key, splice->prev_[i + 1], splice->next_[i + 1],
                                i, &splice->prev_[i], &splice->next_[i]);
  }
  co_return;
}

task<bool> Client::Insert(Node* x, Splice* splice,
                          bool allow_partial_splice_fix) {
  uint64_t height = x->UnstashHeight();
  uint64_t* max_height = co_await GetMaxHeight(seg_rmr.raddr);
  while (height > *max_height) {
    if (!co_await cli->cas(seg_rmr.raddr, *max_height, height)) {
      *max_height = height;
    }
  }

  int recompute_height = 0;
  if (splice->height_ < *max_height) {
    splice->height_ = *max_height;
    recompute_height = *max_height;
    splice->prev_[*max_height] = GetHead();
    splice->next_[*max_height] = 0;
  } else {
    while (recompute_height < *max_height) {
      if (co_await NodeNext(splice->prev_[recompute_height],
                            recompute_height) !=
          splice->next_[recompute_height]) {
        ++recompute_height;
      } else if (splice->prev_[recompute_height] != GetHead() &&
                 !co_await KeyIsAfterNode(*x->Key(),
                                          splice->prev_[recompute_height])) {
        if (allow_partial_splice_fix) {
          uintptr_t bad = splice->prev_[recompute_height];
          while (splice->prev_[recompute_height] == bad) {
            ++recompute_height;
          }
        } else {
          recompute_height = *max_height;
        }
      } else if (co_await KeyIsAfterNode(*x->Key(),
                                         splice->next_[recompute_height])) {
        if (allow_partial_splice_fix) {
          uintptr_t bad = splice->next_[recompute_height];
          while (splice->next_[recompute_height] == bad) {
            ++recompute_height;
          }
        } else {
          recompute_height = *max_height;
        }
      } else {
        break;
      }
    }
  }
  if (recompute_height > 0) {
    co_await RecomputeSpliceLevels(*x->Key(), splice, recompute_height);
  }

  uintptr_t node = ralloc.alloc(sizeof(uintptr_t) * (height - 1) +
                                sizeof(Node) + 2 * sizeof(int64_t)) +
                   sizeof(uintptr_t) * (height - 1);
  co_await cli->write(node + sizeof(Node), x + sizeof(Node),
                      2 * sizeof(int64_t), lmr->lkey);
  log_err(
      "nodekey=%ld,"
      "nodevalue:%ld,xkey=%ld,xvalue:%ld\n",
      co_await NodeKey(node), co_await NodeValue(node), *x->Key(), *x->Value());
  bool splice_is_vaild = true;
  for (int i = 0; i < height; ++i) {
    while (true) {
      if (i == 0 && splice->prev_[0] != GetHead() &&
          (!co_await KeyIsAfterNode(*x->Key(), splice->prev_[0]))) {
        co_return false;
      }
      if (i == 0 && splice->next_[0] != 0 &&
          (!co_await KeyIsBeforeNode(*x->Key(), splice->next_[0]))) {
        co_return false;
      }
      co_await NodeSetNext(node, i, splice->next_[i]);
      if (co_await NodeCASNext(splice->prev_[i], i, splice->next_[i], node)) {
        log_err(
            "i=%d,prev_=%lu,next_=%lu,node=%lu,prev_next=%lu,nodekey=%ld,"
            "nodevalue:%ld,xkey=%ld,xvalue:%ld\n",
            i, splice->prev_[i], splice->next_[i], node,
            co_await NodeNext(splice->prev_[i], 0), co_await NodeKey(node),
            co_await NodeValue(node), *x->Key(), *x->Value());
        break;
      }
      co_await FindSpliceForLevel(*x->Key(), splice->prev_[i], 0, i,
                                  &splice->prev_[i], &splice->next_[i]);
    }
    if (i > 0) {
      splice_is_vaild = false;
    }
  }
  if (splice_is_vaild) {
    for (int i = 0; i < height; ++i) {
      splice->prev_[i] = node;
    }
  } else {
    splice->height_ = 0;
  }
  co_return true;
}

task<uintptr_t> Client::FindGreaterOrEqual(const int64_t key) {
  uintptr_t now = GetHead();
  uintptr_t last_bigger = 0;
  int level = *co_await GetMaxHeight(seg_rmr.raddr) - 1;
  while (true) {
    uintptr_t next = co_await NodeNext(now, level);
    if (next == 0 || next == last_bigger ||
        !co_await KeyIsAfterNode(key, next)) {
      if (Equal(key, co_await NodeKey(next)) || level == 0) {
        co_return next;
      } else {
        --level;
      }
    } else {
      now = next;
    }
  }
}

task<uintptr_t> Client::Search(const int64_t key) {
  uintptr_t x = co_await FindGreaterOrEqual(key);
  co_return x != 0 && Equal(co_await NodeKey(x), key) ? x : 0;
}

task<> Client::Print() {
  uintptr_t now = co_await NodeNext(GetHead(), 0);
  while (now != 0) {
    log_err("now=%lu\n", now);
    printf("%ld %ld\n", co_await NodeKey(now), co_await NodeValue(now));
    now = co_await NodeNext(now, 0);
    alloc.ReSet(0);
  }
  putchar('\n');
}

}  // namespace SKIPLIST
