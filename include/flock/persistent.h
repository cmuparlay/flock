#pragma once
#include "log.h"
#include "timestamps.h"

#define bad_ptr ((1ul << 48) -1)

using IT = size_t;
struct persistent {
  std::atomic<TS> time_stamp;
  IT next;
  persistent() : time_stamp(0), next(bad_ptr) {}
};

memory_pool<persistent> empty_pool;

template <typename V>
struct persistent_ptr {
  // private:
  using TV = tagged<V*>;
  std::atomic<IT> v;

  // sets the timestamp in a version link if time stamp is TBD
  static IT set_stamp(IT newv) {
    V* x = strip_tag(newv);
    if ((x != nullptr) && x->time_stamp.load() == tbd) {
      TS ts = global_stamp.get_write_stamp();
      long old = tbd;
      x->time_stamp.compare_exchange_strong(old, ts);
    }
    return newv;
  }

  IT get_val(Log &p) {
    return set_stamp(p.commit_value(v.load()).first);
  }

  // uses lowest bit to "mark" a "nullptr"
  // highest bits are used by the ABA "tag"
  // required to support using store with a nullptr
  // such a store creates a version link and stores a pointer to that link
  // the link is tagged with a bit to indicate it is a "nullptr"
  static V* add_tag(V* ptr) {return (V*) (1ul | (IT) ptr);};
  static bool is_empty(IT ptr) {return ptr & 1;}
  static bool is_null(IT ptr) {return TV::value(ptr) == nullptr || is_empty(ptr);}
  static V* strip_tag(IT ptr) {return TV::value(ptr & ~1ul);}
  static V* get_ptr(IT ptr) { return is_empty(ptr) ? nullptr : TV::value(ptr);}

  template <typename Lock>
  V* read_fix(Lock* lck) {
    IT ptr = v.load();
    set_stamp(ptr);     // ensure time stamp is set
    if (is_empty(ptr)) {
      V* ptr_notag = strip_tag(ptr);
      if (ptr_notag->time_stamp.load() > 0)
	lck->try_with_lock([=] {
	  if (TV::cas(v, ptr, nullptr))
	    empty_pool.pool.retire((persistent*) ptr_notag);
	  return true;});
      return nullptr;
    }
    return strip_tag(ptr);
  }

public:

  persistent_ptr(V* v) : v(TV::init(v)) {}
  persistent_ptr(): v(TV::init(0)) {}
  void init(V* vv) {v = TV::init(vv);}
  V* load() {
    if (local_stamp != -1) return read();
    else return get_ptr(get_val(lg));}

  // reads snapshotted version
  V* read() {
    IT head = v.load();
    set_stamp(head);     // ensure time stamp is set
    TS ls = local_stamp;
    if (ls != -1) 
      // chase down version chain
      while (head != 0 && strip_tag(head)->time_stamp.load() > ls)
	head = (IT) strip_tag(head)->next;
    return get_ptr(head);
  }

  V* read_() { return get_ptr(v.load());}
  void validate() { set_stamp(v.load());}
  
  void store(V* newv) {
    V* newv_tagged = newv;
    IT oldv_tagged = get_val(lg);
    V* oldv = strip_tag(oldv_tagged);

    // if newv is null we need to allocate a version link for it and mark it
    if (newv == nullptr) {
      newv = (V*) empty_pool.pool.new_obj();
      newv_tagged = add_tag(newv);
    }
    newv->time_stamp = tbd;
    newv->next = (IT) oldv_tagged;
    //bool succeeded = v.compare_exchange_strong(oldv_tagged, (IT) newv_tagged);
    bool succeeded = TV::cas(v, oldv_tagged, newv_tagged);
    if (succeeded && is_empty(oldv_tagged))
      empty_pool.pool.retire((persistent*) oldv);
    set_stamp((IT) newv);
    // shortcut if appropriate
    if (oldv != nullptr && newv->time_stamp == oldv->time_stamp)
      newv->next = oldv->next;
  }
  V* operator=(V* b) {store(b); return b; }
};
