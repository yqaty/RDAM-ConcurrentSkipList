#pragma once
#include <fcntl.h>
#include <math.h>

#include <cassert>
#include <chrono>
#include <map>
#include <tuple>
#include <vector>

#include "aiordma.h"
#include "alloc.h"
#include "config.h"
#include "kv_trait.h"
#include "perf.h"
#include "search.h"

#define WO_WAIT_WRITE

namespace SKIPLIST {

constexpr uint16_t kMaxHeight_ = 12;
constexpr uint16_t kBranching_ = 4;
constexpr uint32_t kScaledInverseBranching_ = (1ul << 30) / (kBranching_);
constexpr uint64_t SEGMENT_SIZE = 1024;
constexpr uint64_t SLOT_PER_SEG =
    ((SEGMENT_SIZE) / (sizeof(uint64_t) + sizeof(uint8_t)));
constexpr uint64_t SLOT_BATCH_SIZE = 8;
constexpr uint64_t RETRY_LIMIT =
    (SLOT_PER_SEG / SLOT_BATCH_SIZE);  // TODO : 后期试试改成其他较小的值
constexpr uint64_t MAX_MAIN_SIZE = 64 * SLOT_PER_SEG;
constexpr uint64_t MAX_FP_INFO = 256;
constexpr uint64_t INIT_DEPTH = 4;
constexpr uint64_t MAX_DEPTH = 16;
constexpr uint64_t DIR_SIZE = (1 << MAX_DEPTH);
constexpr uint64_t ALIGNED_SIZE =
    64;  // aligned size of len bitfield in DepSlot
constexpr uint64_t dev_mem_size = (1 << 10) * 64;  // 64KB的dev mem，用作lock
constexpr uint64_t num_lock =
    (dev_mem_size - sizeof(uint64_t)) /
    sizeof(uint64_t);  // Lock数量，client对seg_id使用hash来共享lock

class Client : public BasicDB {
 public:
  struct Node;
  struct Splice;
  Client(Config& config, ibv_mr* _lmr, rdma_client* _cli, rdma_conn* _conn,
         rdma_conn* _wowait_conn, uint64_t _machine_id, uint64_t _cli_id,
         uint64_t _coro_id);

  Client(const Client&) = delete;

  ~Client();

  task<int32_t> NodeKey(uintptr_t node);

  task<int32_t> NodeValue(uintptr_t node);

  Node* AllocateKeyAndValue(const int32_t key, const int32_t value,
                            Client* cli);

  Splice* AllocateSpliceOnHeap();

  task<bool> InsertWithHintConcurrently(Node* x, void** hint);

  task<bool> InsertConcurrently(Node* x);

  task<bool> Client::Insert(Node* x, Splice* splice,
                            bool allow_partial_splice_fix);

  task<uintptr_t> Search(const int32_t key);

  task<> Print();

 private:
  task<uint16_t> GetMaxHeight(uint64_t raddr);

  int RandomHeight();

  uintptr_t GetHead();

  Client::Node* Client::AllocateNode(const int32_t key, const int32_t value,
                                     const int height);

  task<uintptr_t> NodeNext(uintptr_t node, int n);

  task<> NodeSetNext(uintptr_t node, int n, uintptr_t* x);

  task<bool> NodeCASNext(uintptr_t node, int n, uintptr_t expected,
                         uintptr_t* x);

  bool Equal(const int32_t& a, const int32_t& b) const;

  bool LessThan(const int32_t& a, const int32_t& b) const;

  task<bool> KeyIsAfterNode(const int32_t key, uintptr_t node);

  task<bool> KeyIsBeforeNode(const int32_t key, uintptr_t node);

  task<uintptr_t> FindGreaterOrEqual(const int32_t key);

  task<> FindSpliceForLevel(const int32_t key, uintptr_t before,
                            uintptr_t after, int level, uintptr_t* out_prev,
                            uintptr_t* out_next);

  task<> RecomputeSpliceLevels(const int32_t key, Splice* splice,
                               int recompute_level);

 private:
  // rdma structs
  rdma_client* cli;
  rdma_conn* conn;
  rdma_conn* wo_wait_conn;
  rdma_rmr seg_rmr;
  struct ibv_mr* lmr;

  Alloc alloc;
  RAlloc ralloc;
  uint64_t machine_id;
  uint64_t cli_id;
  uint64_t coro_id;
  uint64_t key_num;
  uint64_t key_off;

  // Statistic
  Perf perf;
  SumCost sum_cost;
  uint64_t op_cnt;
  uint64_t miss_cnt;
  uint64_t retry_cnt;
};

struct Client::Splice {
  int height_ = 0;
  uintptr_t* prev_;
  uintptr_t* next_;
};

struct Client::Node {
  void StashHeight(const uint16_t height);

  uint16_t UnstashHeight() const;

  int32_t* Key() const;

  int32_t* Value() const;

 private:
  uint64_t next_[1];
};

class Server : public BasicDB {
 public:
  Server(Config& config);
  ~Server();

 private:
  void Init();

  rdma_dev dev;
  rdma_server ser;
  struct ibv_mr* seg_mr;
  ibv_dm* lock_dm;  // Locks for Segments
  ibv_mr* lock_mr;
  char* mem_buf;

  Alloc alloc;
};

}  // namespace SKIPLIST