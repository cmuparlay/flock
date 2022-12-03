#pragma once

#include "flock/flock.h"
#include "timestamps.h"

namespace vl {

parlay::sequence<long> i_counts(parlay::num_workers()*16, 0);
void print_counts() {
  std::cout << " indirect = " << parlay::reduce(i_counts) << std::endl;
}

using IT = void*;

struct versioned {
  std::atomic<TS> time_stamp;
  IT next_version;
  static constexpr size_t init_ptr = (1ul << 48) - 2;
  TS read_stamp() {return time_stamp.load();}
  TS load_stamp() {return flck::commit(time_stamp.load());}
  void set_stamp(TS t) {
    TS old = tbd;
    if(time_stamp.load() == tbd)
      time_stamp.compare_exchange_strong(old, t);
  }
  versioned() : time_stamp(tbd), next_version((IT) init_ptr) {}
  versioned(IT next) : time_stamp(tbd), next_version(next) {}
};

struct plink : versioned {
  void* value;
  plink(IT next, void* value) : versioned{next}, value(value) {}  
};

 flck::memory_pool<plink> link_pool;

template <typename V>
struct versioned_ptr {
private:
  flck::atomic<IT> v;

  // uses lowest bit of pointer to indicate whether indirect (1) or not (0)
  static V* add_indirect_mark(IT ptr) {return (V*) (1ul | (size_t) ptr);};
  static bool is_indirect(IT ptr) {return (size_t) ptr & 1;}
  static V* strip_mark(IT ptr) {return (V*) ((size_t) ptr & ~1ul);}

  void shortcut(IT ptr) {
#ifndef NoShortcut
    plink* ptr_u = (plink*) strip_mark(ptr);
    if (ptr_u->read_stamp() <= done_stamp) {
#ifdef NoHelp
      if (v.cas(ptr, (IT) ptr_u->value))
	link_pool.retire(ptr_u);
#else
      if (v.cas_ni(ptr, (IT) ptr_u->value))
	link_pool.retire_ni(ptr_u);
#endif
    }
#endif
  }

  V* get_ptr_shortcut(IT ptr) {
    plink* ptr_u = (plink*) strip_mark(ptr);
    if (is_indirect(ptr)) {
      shortcut(ptr);
      return (V*) ptr_u->value;
    } else return (V*) ptr_u;
  }

  static IT set_stamp(IT ptr) {
    V* ptr_u = strip_mark(ptr);
    if (ptr != nullptr && ptr_u->read_stamp() == tbd)
      ptr_u->set_stamp(global_stamp.get_write_stamp());
    return ptr;
  }

  static IT set_zero_stamp(V* ptr) {
    if (ptr != nullptr && ptr->read_stamp() == tbd)
      ptr->time_stamp = zero_stamp;
    return (IT) ptr;
  }

public:

  versioned_ptr(): v(nullptr) {}
  versioned_ptr(V* ptr) : v(set_zero_stamp(ptr)) {}

  ~versioned_ptr() {
    IT ptr = v.load();
    if (is_indirect(ptr))
      link_pool.destruct((plink*) strip_mark(ptr));
  }

  void init(V* ptr) {v = set_zero_stamp(ptr);}

  V* read_snapshot() {
    TS ls = local_stamp;
    IT head = v.read();
    set_stamp(head);
    //if (is_indirect(head)) shortcut(head);

    V* head_unmarked = strip_mark(head);

    // chase down version chain
    while (head != nullptr && head_unmarked->read_stamp() > ls) {
      head = head_unmarked->next_version;
      head_unmarked = strip_mark(head);
    }
#ifdef LazyStamp
    if (head != nullptr && head_unmarked->read_stamp() == ls
	&& speculative)
      aborted = true;
#endif
    if (is_indirect(head)) {
      //shortcut(head);
      return (V*) ((plink*) head_unmarked)->value;
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
    IT old_v = v.load();
    IT new_v = (IT) ptr;
    bool use_indirect = (ptr == nullptr || ptr->load_stamp() != tbd);

    if (use_indirect) 
      new_v = add_indirect_mark((V*) link_pool.new_obj(old_v, new_v));
    else ptr->next_version = old_v;

#ifdef NoShortcut
    v = new_v;
    if (is_indirect(old_v))
      link_pool.retire((plink*) strip_mark(old_v));
#else
    v.cam(old_v, new_v);
    if (is_indirect(old_v)) {
      IT val = v.load();
      if (val != (V*) ((plink*) strip_mark(old_v))->value)
	link_pool.retire((plink*) strip_mark(old_v));
      else v.cam(val, new_v);
    }
#endif
    set_stamp(new_v);
    if (use_indirect) shortcut(new_v);
  }
  
  bool cas(V* expv, V* newv) {
#ifndef NoShortcut
    for(int ii = 0; ii < 2; ii++) {
#endif
      IT old_v = v.load();
      V* old = get_ptr_shortcut(old_v);
      set_stamp(old_v);
      if(old != expv) return false;
      if (old == newv) return true;
      bool use_indirect = (newv == nullptr || newv->load_stamp() != tbd);
      IT new_v = (IT) newv;

      if(use_indirect)
	new_v = add_indirect_mark((V*) link_pool.new_obj(old_v, new_v));
      else newv->next_version = old_v;

      bool succeeded = v.cas_ni(old_v, new_v);

      if(succeeded) {
        set_stamp(new_v);
        if (is_indirect(old_v))
	  link_pool.retire((plink*) strip_mark(old_v));
#ifndef NoShortcut
	if (use_indirect) shortcut(new_v);
#endif
        return true;
      }
      if (use_indirect) link_pool.destruct((plink*) strip_mark(new_v));
#ifndef NoShortcut
    }
#endif
    set_stamp(v.load());
    return false;
  }

  V* operator=(V* b) {store(b); return b; }
};
} // namespace vl
