// A version for data structures that only record once.
// Effectively this means that an object that is pointed to
// by a persistent_ptr can only be stored to a persistent pointer
// once.

// Based on paper:
// Yuanhao Wei, Naama Ben-David, Guy E. Blelloch, Panagiota Fatourou, Eric Ruppert, Yihan Sun
// Constant-time snapshots with applications to concurrent data structures
// PPoPP 2021

#pragma once
#include "timestamps.h"
#ifdef NoHelp
template <typename F>
bool skip_if_done_no_log(F f) {f(); return true;}
#else
#include "lf_log.h"
#endif

#define bad_ptr ((void*) ((1ul << 48) -1))
struct persistent {
  std::atomic<TS> time_stamp;
  void* next;
  persistent() : time_stamp(tbd), next(bad_ptr) {}
};

template <typename V>
struct persistent_ptr {
private:
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

#ifdef NoHelp
  V* get_val() {return set_stamp(v.load());}
  static TS get_stamp(V* u) { return u->time_stamp.load();}
#else
  V* get_val() {
    return set_stamp(lg.commit_value_safe(v.load()).first);}
  static TS get_stamp(V* u) {
    return lg.commit_value(u->time_stamp.load()).first;}
#endif

public:

  persistent_ptr(V* v) : v(v) {}
  persistent_ptr(): v(nullptr) {}
  void init(V* vv) {v = vv;}

  // reads snapshotted version (ls >= 0)
  V* read_snapshot() {
    // ensure time stamp is set
    V* head = set_stamp(v.load());
    TS ls = local_stamp;
    while (head->time_stamp.load() > ls)
      head = (V*) head->next;
    return head;
  }

  V* load() {
    if (local_stamp != -1) return read_snapshot();
    else return get_val();}

  V* read() {
    if (local_stamp != -1) return read_snapshot();
    else return v.load();}

  void validate() { set_stamp(v.load());}
  
  void store(V* newv) {
    V* oldv = get_val();
    // check that newv is only recorded once
    if (get_stamp(newv) != tbd) {
      std::cout << "recording a second time not allowed" << std::endl;
      abort();
    }

    skip_if_done_no_log([&] { // for efficiency, correct without it
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
