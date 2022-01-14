// a version for aba_free data structures
// pointers cannot be null

// Based on paper:
// Yuanhao Wei, Naama Ben-David, Guy E. Blelloch, Panagiota Fatourou, Eric Ruppert, Yihan Sun
// Constant-time snapshots with applications to concurrent data structures
// PPoPP 2021

#pragma once
#include "log.h"
#include "timestamps.h"

#define bad_ptr ((void*) ((1ul << 48) -1))
struct persistent {
  std::atomic<TS> time_stamp;
  void* next;
  persistent() : time_stamp(0), next(bad_ptr) {}
};

template <typename V>
struct persistent_ptr {
  // private:
  std::atomic<V*> v;
  // sets the timestamp in a version link if time stamp is TBD
  static V* set_stamp(V* x) {
    if (x->time_stamp.load() == tbd) {
      TS ts = global_stamp.get_write_stamp();
      long old = tbd;
      x->time_stamp.compare_exchange_strong(old, ts);
    }
    return x;
  }

  V* get_val(Log &p) {
    return set_stamp(p.commit_value_safe(v.load()).first);
  }

public:

  persistent_ptr(V* v) : v(v) {}
  persistent_ptr(): v(nullptr) {}
  void init(V* vv) {v = vv;}
  V* load() {
    if (local_stamp != -1) return read();
    else return get_val(lg);}

  // reads snapshotted version
  V* read() {
    // ensure time stamp is set
    V* head = set_stamp(v.load());
    TS ls = local_stamp;
    if (ls != -1) 
      // chase down version chain
      while (head->time_stamp.load() > ls)
	head = (V*) head->next;
    return head;
  }

  V* read_() { return v.load();}
  void validate() { set_stamp(v.load());}
  
  void store(V* newv) {
    V* oldv = get_val(lg);
    skip_if_done([&] { // for efficiency, correct without it
      newv->time_stamp = tbd;
      newv->next = (void*) oldv;
      v.compare_exchange_strong(oldv, newv);
      set_stamp(newv);
      // shortcut if appropriate
      if (oldv != nullptr && newv->time_stamp == oldv->time_stamp)
	newv->next = oldv->next;
    });
  }
  V* operator=(V* b) {store(b); return b; }
};
