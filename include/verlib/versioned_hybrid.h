#pragma once

#include "flock/flock.h"
#include "timestamps.h"

namespace verlib {

struct ver_link;

struct versioned {
  flck::atomic_write_once<TS> time_stamp;
  ver_link* next_version;
  static constexpr size_t init_ptr = (1ul << 48) - 2;
  versioned() : time_stamp(tbd), next_version((ver_link*) init_ptr) {}
  versioned(ver_link* next) : time_stamp(tbd), next_version(next) {}
};

struct ver_link : versioned {
  void* value;
  ver_link(ver_link* next, void* value) : versioned{next}, value(value) {}  
};

flck::memory_pool<ver_link> link_pool;

template <typename V>
struct versioned_ptr {
private:
  flck::atomic<ver_link*> v;

  // uses lowest bit of pointer to indicate whether indirect (1) or not (0)
  static ver_link* add_indirect(ver_link* ptr) {
    return (ver_link*) (1ul | (size_t) ptr);};
  static ver_link* strip_indirect(ver_link* ptr) {
    return (ver_link*) ((size_t) ptr & ~1ul);}
  static bool is_indirect(ver_link* ptr) {
    return (size_t) ptr & 1;}

  void shortcut(ver_link* ptr) {
#ifndef NoShortcut
    ver_link* ptr_ = strip_indirect(ptr);
    if (ptr_->time_stamp.load_ni() <= done_stamp) {
#ifdef NoHelp
      if (v.cas(ptr, (ver_link*) ptr_->value))
	link_pool.retire(ptr_);
#else
      if (v.cas_ni(ptr, (ver_link*) ptr_->value))
	link_pool.retire_ni(ptr_);
#endif
    }
#endif
  }

  V* get_ptr_shortcut(ver_link* ptr) {
    ver_link* ptr_ = strip_indirect(ptr);
    if (is_indirect(ptr)) {
      shortcut(ptr);
      return (V*) ptr_->value;
    } else return (V*) ptr_;
  }

  static ver_link* set_stamp(ver_link* ptr) {
    ver_link* ptr_ = strip_indirect(ptr);
    if (ptr != nullptr && ptr_->time_stamp.load_ni() == tbd) {
      TS t = global_stamp.get_write_stamp();
      if(ptr_->time_stamp.load_ni() == tbd)
        ptr_->time_stamp.cas_ni(tbd, t);
    }
    return ptr;
  }

  static ver_link* set_zero_stamp(V* ptr) {
    if (ptr != nullptr && ptr->time_stamp.load_ni() == tbd)
      ptr->time_stamp = zero_stamp;
    return (ver_link*) ptr;
  }

  bool cas_from_cam(ver_link* old_v, ver_link* new_v) {
#ifdef NoHelp
    return v.cas(old_v, new_v);
#else
    v.cam(old_v, new_v);
    return (v.load() == new_v ||
	    strip_indirect(new_v)->time_stamp.load() != tbd);
#endif
  }

public:

  versioned_ptr(): v(nullptr) {}
  versioned_ptr(V* ptr) : v(set_zero_stamp(ptr)) {}

  ~versioned_ptr() {
    ver_link* ptr = v.load();
    if (is_indirect(ptr))
      link_pool.destruct(strip_indirect(ptr));
  }

  void init(V* ptr) {v = set_zero_stamp(ptr);}

  V* read_snapshot() {
    TS ls = local_stamp;
    ver_link* head = set_stamp(v.read());
    ver_link* head_unmarked = strip_indirect(head);

    // chase down version chain
    while (head != nullptr && head_unmarked->time_stamp.load() > ls) {
      head = head_unmarked->next_version;
      head_unmarked = strip_indirect(head);
    }
#ifdef LazyStamp
    if (head != nullptr && head_unmarked->time_stamp.load() == ls
	&& speculative)
      aborted = true;
#endif
    if (is_indirect(head)) {
      return (V*) head_unmarked->value;
    } else return (V*) head;
  }

  V* load() {  // can be used anywhere
    if (local_stamp != -1) return read_snapshot();
    else return get_ptr_shortcut(set_stamp(v.load()));
  }
  
  V* read() {  // only safe on journey
    return get_ptr_shortcut(v.read());
  }

  void validate() {
    set_stamp(v.load());     // ensure time stamp is set
  }
  
  void store(V* ptr) {
    ver_link* old_v = v.load();
    ver_link* new_v = (ver_link*) ptr;
    bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

    if (use_indirect) 
      new_v = add_indirect(link_pool.new_obj(old_v, new_v));
    else ptr->next_version = old_v;

#ifdef NoShortcut
    v = new_v;
    if (is_indirect(old_v))
      link_pool.retire(strip_indirect(old_v));
#else
    v.cam(old_v, new_v);
    if (is_indirect(old_v)) {
      ver_link* val = v.load();
      if (val != strip_indirect(old_v)->value)
	link_pool.retire(strip_indirect(old_v));
      else v.cam(val, new_v);
    }
#endif
    set_stamp(new_v);
    if (use_indirect) shortcut(new_v);
  }
  
  bool cas(V* exp, V* ptr) {
#ifndef NoShortcut
    for(int ii = 0; ii < 2; ii++) {
#endif
      ver_link* old_v = v.load();
      ver_link* new_v = (ver_link*) ptr;
      V* old = get_ptr_shortcut(old_v);
      set_stamp(old_v);
      if(old != exp) return false;
      if (exp == ptr) return true;
      bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

      if(use_indirect)
	new_v = add_indirect(link_pool.new_obj(old_v, new_v));
      else ptr->next_version = old_v;

      if (cas_from_cam(old_v, new_v)) {
        set_stamp(new_v);
        if (is_indirect(old_v))
	  link_pool.retire(strip_indirect(old_v));
#ifndef NoShortcut
	if (use_indirect) shortcut(new_v);
#endif
        return true;
      }
      if (use_indirect) link_pool.destruct(strip_indirect(new_v));
#ifndef NoShortcut
    }
#endif
    set_stamp(v.load());
    return false;
  }

  V* operator=(V* b) {store(b); return b; }
};
} // namespace verlib
