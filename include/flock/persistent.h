#include "log.h"
#include <sched.h>

#pragma once

void inc_backoff(int &bk, int max) {if (bk < max) bk = round(1.1*bk);}
void dec_backoff(int &bk) {if (bk > 1) bk = round(.9*bk);}

using TS = long;

struct timestamp_simple {
  std::atomic<TS> stamp;
  static constexpr int delay = 800;

  TS get_read_stamp() {
    TS ts = stamp.load();

    // delay to reduce contention
    for(volatile int i = 0; i < delay; i++);

    // only update timestamp if has not changed
    if (stamp.load() == ts)
      stamp.compare_exchange_strong(ts,ts+1);

    return ts;
  }

  TS get_write_stamp() {return stamp.load();}
  timestamp_simple() : stamp(0) {}
};

struct timestamp_multiple {
  static constexpr int slots = 4;
  static constexpr int gap = 16;
  static constexpr int delay = 300;
  std::atomic<TS> stamps[slots*gap];

  inline TS get_write_stamp() {
    TS total = 0;
    for (int i = 0; i < slots; i++)
      total += stamps[i*gap].load();
    return total;
  }

  TS get_read_stamp() {
    TS ts = get_write_stamp();
    //int i = sched_getcpu() / 36; //% slots;
    int i = parlay::worker_id() % slots;
    for(volatile int j = 0; j < delay ; j++);
    TS tsl = stamps[i*gap].load();
    if (ts == get_write_stamp()) {
      //int i = parlay::worker_id() % slots;
      stamps[i*gap].compare_exchange_strong(tsl,tsl+1);
    }
    return ts;
  }

  timestamp_multiple() {
    for (int i = 0; i < slots; i++) stamps[i*gap] = 0;
  }
};


// works well if mostly reads or writes
// if stamp is odd then in write mode, and if even in read mode
// if not in the right mode, then increment to put in the right mode
thread_local float read_backoff = 50.0;
thread_local float write_backoff = 1000.0;
struct timestamp_read_write {
  std::atomic<TS> stamp;
  // unfortunately quite sensitive to the delays
  // these were picked empirically for a particular machine (aware)
  //static constexpr int write_delay = 1500;
  //static constexpr int read_delay = 150;
  
  inline TS get_write_stamp() {
    TS s = stamp.load();
    if (s % 2 == 1) return s;
    //for(volatile int j = 0; j < write_delay ; j++);
    for(volatile int j = 0; j < round(write_backoff) ; j++);
    if (s != stamp.load()) return s;
    if (stamp.compare_exchange_strong(s,s+1)) {
      if (write_backoff > 1200.0) write_backoff *= .98;
    } else if (write_backoff < 1800.0) write_backoff *= 1.02;
    return s+1; // return new stamp
  }

  TS get_read_stamp() {
    TS s = stamp.load();
    if (s % 2 == 0) return s;
    for(volatile int j = 0; j < round(read_backoff) ; j++);
    if (s != stamp.load()) return s;
    if (stamp.compare_exchange_strong(s,s+1)) {
      if (read_backoff > 10.0) read_backoff *= .98;
    } else if (read_backoff < 400.0) read_backoff *= 1.02;
    return s; // return old stamp
  }

  timestamp_read_write() : stamp(0) {}
};

timestamp_read_write global_stamp;
thread_local TS local_stamp{-1};
#define bad_ptr ((void*) ((1ul << 48) -1))
const TS tbd = -1;

template <typename F>
auto with_snapshot(F f) {
  return with_epoch([&] {
    local_stamp = global_stamp.get_read_stamp();
    auto r = f();
    local_stamp = -1;
    return r;
  });
}

struct persistent {
  std::atomic<TS> time_stamp;
  void* next;
  persistent() : time_stamp(0), next(bad_ptr) {}
};

memory_pool<persistent> empty_pool;

template <typename V>
struct persistent_ptr {
  // private:
  std::atomic<V*> v;

  // sets the timestamp in a version link if time stamp is TBD
  static V* set_stamp(V* newv) {
    V* x = strip_tag(newv);
    if ((x != nullptr) && x->time_stamp.load() == tbd) {
      TS ts = global_stamp.get_write_stamp();
      long old = tbd;
      x->time_stamp.compare_exchange_strong(old, ts);
    }
    return newv;
  }

  V* get_val(Log &p) {
    return set_stamp(p.commit_value_safe(v.load()).first);
  }

  // tags a "nullptr"
  // required to support using store with a nullptr
  // such a store creates a version link and stores a pointer to that link
  // the link is tagged with a bit to indicate it is a "nullptr"
  static V* add_tag(V* ptr) {return (V*) ((1ul << 63) | (size_t) ptr);};
  static bool is_empty(V* ptr) {return (((size_t) ptr) >> 63) == 1;}
  static bool is_null(V* ptr) {return ptr == nullptr || is_empty(ptr);}
  static V* strip_tag(V* ptr) {return (V*) (((size_t) ptr) & ((1ul << 63) - 1));}
  static V* get_ptr(V* ptr) { return is_empty(ptr) ? nullptr : ptr;}

  V* fix_ptr() {
    V* ptr = set_stamp(v.load());
    if (is_empty(ptr)) {
      V* ptr_notag = strip_tag(ptr);
      if (ptr_notag->time_stamp.load() < 0)
	if (v.compare_exchange_strong(ptr,nullptr))
	  empty_pool.pool.retire((persistent*) ptr_notag);
      return nullptr;
    } return ptr;
  }

public:

  persistent_ptr(V* v) : v(v) {}
  persistent_ptr(): v(nullptr) {}
  void init(V* vv) {v = vv;}
  V* load() {
    if (local_stamp != -1) return read();
    else return get_ptr(get_val(lg));}

  // reads snapshotted version
  V* read() {
    // ensure time stamp is set
    V* head = set_stamp(v.load());
    TS ls = local_stamp;
    if (ls != -1) 
      // chase down version chain
      while (head != nullptr && strip_tag(head)->time_stamp.load() > ls)
	head = (V*) strip_tag(head)->next;
    return get_ptr(head);
  }

  V* read_() { return get_ptr(v.load());}
  //V* read_() { return fix_ptr();}
  //V* read_() { return read();}
  void validate() { set_stamp(v.load());}
  //void validate() {}
  
  void store(V* newv) {
    V* newv_tagged = newv;
    V* oldv_tagged = get_val(lg);
    V* oldv = strip_tag(oldv_tagged);
    skip_if_done([&] { // for efficiency, correct without it
      // if newv is null we need to allocate a version link for it
      // and tag it
      if (newv == nullptr) {
	newv = (V*) empty_pool.pool.new_obj();
	newv_tagged = add_tag(newv);
      }

      //auto nxt = (V*) lg.commit_value_safe(newv->next).first;
      //assert(nxt == bad_ptr);
      newv->time_stamp = tbd;
      newv->next = (void*) oldv_tagged;
      bool succeeded = v.compare_exchange_strong(oldv_tagged, newv_tagged);
      if (succeeded && is_empty(oldv_tagged))
        empty_pool.pool.retire((persistent*) oldv);
      set_stamp(newv);
      // shortcut if appropriate
      if (oldv != nullptr && newv->time_stamp == oldv->time_stamp)
	newv->next = oldv->next;
    });
  }
  V* operator=(V* b) {store(b); return b; }
};
