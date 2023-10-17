#include "rdma_skiplist.h"

#include "macro.h"
#include "random.h"

namespace SKIPLIST {

Server::Server(Config& config) : dev(nullptr, 1, config.roce_flag), ser(dev) {
  seg_mr = dev.reg_mr(233, config.mem_size);
  auto [dm, mr] = dev.reg_dmmr(234, dev_mem_size);
  lock_dm = dm;
  lock_mr = mr;

  alloc.Set((char*)seg_mr->addr, seg_mr->length);
  Init();
  log_err("init");

  // Init locks
  char tmp[dev_mem_size] = {};  // init locks to zero
  lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
  log_err("memset");

  ser.start_serve();
}

void Server::Init() {
  char* raw = alloc.alloc(sizeof(uint16_t) + sizeof(uintptr_t) * (kMaxHeight_));
  memset(raw, 0, sizeof(uint16_t) + sizeof(uintptr_t) * (kMaxHeight_));
  *reinterpret_cast<uint16_t*>(raw) = 1;
}

Server::~Server() {
  rdma_free_mr(seg_mr);
  rdma_free_dmmr({lock_dm, lock_mr});
}

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
  uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 20) * 200) /
                       (config.num_machine * config.num_cli *
                        config.num_coro);  // 头部保留5GB，其他的留给client
  uint64_t buf_id = config.machine_id * config.num_cli * config.num_coro +
                    cli_id * config.num_coro + coro_id;
  uintptr_t remote_ptr =
      seg_rmr.raddr + seg_rmr.rlen - rbuf_size * buf_id;  // 从尾部开始分配
  ralloc.SetRemote(remote_ptr, rbuf_size, seg_rmr.raddr, seg_rmr.rlen);
  ralloc.alloc(ALIGNED_SIZE);  // 提前分配ALIGNED_SIZE，免得读取的时候越界
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

void Client::Node::StashHeight(const uint16_t height) {
  memcpy(static_cast<void*>(&next_[0]), &height, sizeof(height));
}

uint16_t Client::Node::UnstashHeight() const {
  uint16_t res;
  memcpy(&res, &next_[0], sizeof(res));
  return res;
}

uintptr_t Client::GetHead() {
  return seg_rmr.raddr + sizeof(uint16_t) + sizeof(Node*) * (kMaxHeight_ - 1);
}

int32_t* Client::Node::Key() const {
  return const_cast<int32_t*>(reinterpret_cast<const int32_t*>(&next_[1]));
}

int32_t* Client::Node::Value() const {
  return const_cast<int32_t*>(
      (reinterpret_cast<const int32_t*>(&next_[1]) + 1));
}

task<int32_t> Client::NodeKey(uintptr_t node) {
  char* raw = alloc.alloc(sizeof(int32_t));
  co_await cli->read(node + sizeof(Node), reinterpret_cast<void*>(raw),
                     sizeof(int32_t), lmr->lkey);
  co_return *reinterpret_cast<uint32_t*>(raw);
}

task<int32_t> Client::NodeValue(uintptr_t node) {
  char* raw = alloc.alloc(sizeof(int32_t));
  co_await cli->read(node + sizeof(Node) + sizeof(int32_t),
                     reinterpret_cast<void*>(raw), sizeof(int32_t), lmr->lkey);
  co_return *reinterpret_cast<uint32_t*>(raw);
}

task<uintptr_t> Client::NodeNext(uintptr_t node, int n) {
  uintptr_t next = node - sizeof(Node*) * n;
  char* raw = alloc.alloc(sizeof(Node));
  co_await cli->read(next, reinterpret_cast<void*>(raw), sizeof(Node),
                     lmr->lkey);
  co_return *reinterpret_cast<uintptr_t*>(raw);
}

task<> Client::NodeSetNext(uintptr_t node, int n, uintptr_t* x) {
  co_await cli->write(node - sizeof(Node*) * n, x, sizeof(Node*), lmr->lkey);
  co_return;
}

task<bool> Client::NodeCASNext(uintptr_t node, int n, uintptr_t expected,
                               uintptr_t* x) {
  char* raw = alloc.alloc(sizeof(Node*));
  co_await cli->read(node - sizeof(Node*) * n, raw, sizeof(Node*), lmr->lkey);
  if (*reinterpret_cast<uintptr_t*>(raw) == expected) {
    co_await cli->write(node - sizeof(Node*) * n, x, sizeof(Node*), lmr->lkey);
    co_return true;
  } else {
    co_return false;
  }
}

task<uint16_t> Client::GetMaxHeight(uintptr_t raddr) {
  void* raw = alloc.alloc(sizeof(uint16_t));
  co_await cli->read(raddr, raw, sizeof(uint16_t), lmr->lkey);
  co_return reinterpret_cast<uint16_t>(raw);
}

int Client::RandomHeight() {
  int height = 1;
  while (height < kMaxHeight_ &&
         MySkipList::rand_() < kScaledInverseBranching_) {
    ++height;
  }
  return height;
}

Client::Node* Client::AllocateKeyAndValue(const int32_t key,
                                          const int32_t value, Client* cli) {
  return AllocateNode(key, value, RandomHeight());
}

Client::Node* Client::AllocateNode(const int32_t key, const int32_t value,
                                   const int height) {
  uint32_t pre = sizeof(Node*) * (height - 1);
  char* st = alloc.alloc(pre + sizeof(Node) + sizeof(key) + sizeof(value));
  Node* x = reinterpret_cast<Node*>(st + pre);
  x->StashHeight(height);
  memcpy(st + pre + sizeof(Node), &key, sizeof(key));
  memcpy(st + pre + sizeof(Node) + sizeof(key), &value, sizeof(value));
  return x;
}

Client::Splice* Client::AllocateSpliceOnHeap() {
  size_t array_siz = sizeof(Node*) * (kMaxHeight_ + 1);
  char* raw = new char[sizeof(Splice) + 2 * array_siz];
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<uintptr_t*>(raw + sizeof(Splice));
  splice->next_ =
      reinterpret_cast<uintptr_t*>(raw + sizeof(Splice) + array_siz);
  return splice;
}

bool Client::Equal(const int32_t& a, const int32_t& b) const { return a == b; }

bool Client::LessThan(const int32_t& a, const int32_t& b) const {
  return a < b;
}

task<bool> Client::KeyIsAfterNode(const int32_t key, uintptr_t node) {
  co_return node != 0 && LessThan(co_await NodeKey(node), key);
}

task<bool> Client::KeyIsBeforeNode(const int32_t key, uintptr_t node) {
  co_return node == 0 || LessThan(key, co_await NodeKey(node));
}

task<bool> Client::InsertWithHintConcurrently(Node* x, void** hint) {
  Splice* splice = reinterpret_cast<Splice*>(*hint);
  if (splice == nullptr) {
    splice = AllocateSpliceOnHeap();
    *hint = reinterpret_cast<void*>(splice);
  }
  co_return Insert(x, splice, true);
}

task<bool> Client::InsertConcurrently(Node* x) {
  Splice* splice = AllocateSpliceOnHeap();
  bool bo = co_await Insert(x, splice, false);
  delete[] reinterpret_cast<char*>(splice);
  co_return bo;
}

task<> Client::FindSpliceForLevel(const int32_t key, uintptr_t before,
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

task<> Client::RecomputeSpliceLevels(const int32_t key, Splice* splice,
                                     int recompute_level) {
  for (int i = recompute_level - 1; i >= 0; --i) {
    co_await FindSpliceForLevel(key, splice->prev_[i + 1], splice->next_[i + 1],
                                i, &splice->prev_[i], &splice->next_[i]);
  }
  co_return;
}

task<bool> Client::Insert(Node* x, Splice* splice,
                          bool allow_partial_splice_fix) {
  uint16_t height = x->UnstashHeight();
  uint16_t max_height = co_await GetMaxHeight(seg_rmr.raddr);
  while (height > max_height) {
    char* raw = alloc.alloc(sizeof(uint16_t));
    co_await cli->read(seg_rmr.raddr, raw, sizeof(uint16_t), lmr->lkey);
    uint16_t h = *reinterpret_cast<uint16_t*>(raw);
    if (h == max_height) {
      memcpy(raw, &height, sizeof(uint16_t));
      co_await cli->write(ralloc.r_start, raw, sizeof(uint16_t), lmr->lkey);
      max_height = height;
      break;
    } else {
      memcpy(&height, raw, sizeof(uint16_t));
    }
  }

  int recompute_height = 0;
  if (splice->height_ < max_height) {
    splice->height_ = max_height;
    recompute_height = max_height;
    splice->prev_[max_height] = GetHead();
    splice->next_[max_height] = 0;
  } else {
    while (recompute_height < max_height) {
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
          recompute_height = max_height;
        }
      } else if (co_await KeyIsAfterNode(*x->Key(),
                                         splice->next_[recompute_height])) {
        if (allow_partial_splice_fix) {
          uintptr_t bad = splice->next_[recompute_height];
          while (splice->next_[recompute_height] == bad) {
            ++recompute_height;
          }
        } else {
          recompute_height = max_height;
        }
      } else {
        break;
      }
    }
  }
  if (recompute_height > 0) {
    RecomputeSpliceLevels(*x->Key(), splice, recompute_height);
  }

  uintptr_t node = ralloc.alloc(sizeof(Node*) * (height - 1) + sizeof(Node) +
                                2 * sizeof(int32_t));
  co_await cli->write(node + sizeof(Node), x + sizeof(Node),
                      2 * sizeof(int32_t), lmr->lkey);
  bool splice_is_vaild = true;
  for (int i = 0; i < height; ++i) {
    while (true) {
      if (UNLIKELY(i == 0 && splice->prev_[0] != GetHead() &&
                   (!co_await KeyIsAfterNode(*x->Key(), splice->prev_[0])))) {
        co_return false;
      }
      if (UNLIKELY(i == 0 && splice->next_[0] != 0 &&
                   (!co_await KeyIsBeforeNode(*x->Key(), splice->next_[0])))) {
        co_return false;
      }
      co_await NodeSetNext(node, i, &splice->next_[i]);
      if (co_await NodeCASNext(splice->prev_[i], i, splice->next_[i], &node)) {
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

task<uintptr_t> Client::FindGreaterOrEqual(const int32_t key) {
  uintptr_t now = GetHead();
  uintptr_t last_bigger = 0;
  int level = co_await GetMaxHeight(seg_rmr.raddr) - 1;
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

task<uintptr_t> Client::Search(const int32_t key) {
  uintptr_t x = co_await FindGreaterOrEqual(key);
  co_return x != 0 && Equal(co_await NodeKey(x), key) ? x : 0;
}

task<> Client::Print() {
  uintptr_t now = co_await NodeNext(GetHead(), 0);
  while (now != 0) {
    printf("%d %d\n", NodeKey(now), NodeValue(now));
    now = co_await NodeNext(now, 0);
  }
  putchar('\n');
}

}  // namespace SKIPLIST
