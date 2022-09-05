#pragma once
#ifdef NoHelp
#include "no_tagged.h"
template <typename T>
using tagged = no_tagged<T>;
template <typename F>
bool skip_if_done_(F f) {f(); return true;}
#else
#include "lf_log.h"
#endif

#define bad_ptr ((1ul << 48) -1)

using IT = size_t;
struct persistent {
  std::atomic<TS> time_stamp;
  std::atomic<IT> next_version;
  persistent() : time_stamp(tbd), next_version(bad_ptr) {}
};

struct plink : persistent {
  IT value;
  plink() : persistent() {}
};

mem_pool<plink> link_pool;

template <typename V>
struct persistent_ptr {
private:
  using TV = tagged<V*>;
  std::atomic<IT> v;

  // uses lowest three bits as mark:
  //   2nd bit to indicate it is an indirect pointer
  //   1st bit to indicate it is a null pointer via an indirect pointer (2nd bit also set)
  //   3rd bit to indicate time_stamp has not been set yet
  // the highest 16 bits are used by the ABA "tag"
  static V* add_null_mark(V* ptr) {return (V*) (3ul | (IT) ptr);};
  static V* add_indirect_mark(V* ptr) {return (V*) (2ul | (IT) ptr);};
  static V* add_unset(V* ptr) {return (V*) (4ul | (IT) ptr);};
  static bool is_empty(IT ptr) {return ptr & 1;}
  static bool is_indirect(IT ptr) {return (ptr >> 1) & 1;}
  static bool is_unset(IT ptr) {return (ptr >> 2) & 1;}
  static bool is_null(IT ptr) {return TV::value(ptr) == nullptr || is_empty(ptr);}
  static V* strip_mark_and_tag(IT ptr) {return TV::value(ptr & ~7ul);}
  static V* get_ptr(IT ptr) {
    return (is_indirect(ptr) ?
	    (is_empty(ptr) ? nullptr : (V*) ((plink*) strip_mark_and_tag(ptr))->value) :
	    strip_mark_and_tag(ptr));}
  

  // sets the timestamp in a version link if time stamp is TBD
  // The test for is_unset is an optimization avoiding reading the timestamp
  // if timestamp has been set and the "unset" mark removed from the pointer.
  static IT set_stamp(IT newv) {
    if (is_unset(newv)) {
      V* x = strip_mark_and_tag(newv);
      if ((x != nullptr) && x->time_stamp.load() == tbd) {
	TS ts = global_stamp.get_write_stamp();
	long old = tbd;
	x->time_stamp.compare_exchange_strong(old, ts);
      }
    }
    return newv;
  }

  IT get_val() {
#ifdef NoHelp
    return set_stamp(v.load());
#else
    return set_stamp(lg.commit_value(v.load()).first);
#endif
  }

  // For an indirect pointer if its stamp is older than done_stamp
  // then it will no longer be accessed and can be spliced out.
  // The splice needs to be done under a lock since it can race with updates
  V* shortcut_indirect(IT ptr) {
    if (is_indirect(ptr)) {
      auto ptr_notag = (plink*) strip_mark_and_tag(ptr);
      TS stamp = ptr_notag->time_stamp.load();
      if (stamp <= done_stamp) { 
	      V* newv = is_empty(ptr) ? nullptr : (V*) ptr_notag->value;
        if (TV::cas_with_same_tag(v, ptr, newv, true)) // there can't be an ABA unless indirect node is reclaimed
	         link_pool.retire(ptr_notag);
	      return newv;
      }
    }
    return strip_mark_and_tag(ptr);
  }

public:

  persistent_ptr(V* v) : v(TV::init(v)) {}
  persistent_ptr(): v(TV::init(0)) {}
  void init(V* vv) {v = TV::init(vv);}
  // reads snapshotted version
  V* read_snapshot() {
    TS ls = local_stamp;
    IT head = v.load();
    set_stamp(head);
    // chase down version chain
    while (head != 0 && strip_mark_and_tag(head)->time_stamp.load() > ls)
      head = (IT) strip_mark_and_tag(head)->next_version;
    return get_ptr(head);
  }

  V* load() {
    if (local_stamp != -1) return read_snapshot();
    // Guy : added set_stamp below.  Isn't it needed?
    // Also I tried using shortcut_indirect but it causes a seg fault
    else return get_ptr(set_stamp(get_val()));}

  V* read() {
    // should be shortcut_indirect
    if (local_stamp != -1) read_snapshot();
    return get_ptr(v.load());
  }

  void validate() {
    set_stamp(v.load());     // ensure time stamp is set
  }
  
  void store(V* newv) {
    V* newv_marked = newv;
    IT oldv_tagged = get_val();
    V* oldv = strip_mark_and_tag(oldv_tagged);

    skip_if_done_([&] {
    // if newv is null we need to allocate a version link for it and mark it
    if (newv == nullptr) {
      plink* tmp = link_pool.new_obj();
      tmp->value = 0;
      newv = (V*) tmp;
      newv_marked = add_null_mark(newv);
      newv->next_version = (IT) oldv_tagged;
    } else {
      // if newv has already been recoreded, we need to create a link for it
      // loading timestamp needs to be idempotent
#ifdef NoHelp
      TS ts = newv->time_stamp.load();
#else
      TS ts = lg.commit_value(newv->time_stamp.load()).first;
#endif
      if (ts != tbd) {
      	plink* tmp = link_pool.new_obj();
      	tmp->value = (IT) newv;
      	newv = (V*) tmp;
      	newv_marked = add_indirect_mark(newv);
        newv->next_version = (IT) oldv_tagged;
      } else {
        // TS initial_ts = -1;
        // if(newv->time_stamp.load() == initial_ts)
        //   newv->time_stamp.compare_exchange_strong(initial_ts, tbd);
#ifdef NoHelp
	newv->next_version = (IT) oldv_tagged;
#else	
        IT initial_ptr = bad_ptr;
        if(newv->next_version == initial_ptr)
          newv->next_version.compare_exchange_strong(initial_ptr, (IT) oldv_tagged);
#endif
      }
    }
    // swap in new pointer but marked as "unset" since time stamp is tbd
    bool succeeded = TV::cas(v, oldv_tagged, add_unset(newv_marked));
    IT x = get_val(); // could be avoided if TV::cas returned the tagged version of new

    // if we failed because indirect node got shortcutted out
    if(!succeeded && TV::get_tag(x) == TV::get_tag(oldv_tagged)) { 
      TV::cas(v, x, add_unset(newv_marked));
      x = get_val(); // could be avoided if TV::cas returned the tagged version of new
    }

    // now set the stamp from tbd to a real stamp
    set_stamp((IT) newv);

    // and clear the "unset" mark
    TV::cas(v, x, newv_marked);

    // retire an indirect point if swapped out
    if (succeeded && is_indirect(oldv_tagged))
      link_pool.retire((plink*) oldv);
    
    // shortcut if appropriate, getting rid of redundant time stamps
    // todo: might need to retire if an indirect pointer
    if (oldv != nullptr && newv->time_stamp == oldv->time_stamp)
     newv->next_version.store(oldv->next_version);
		 });
  }
  V* operator=(V* b) {store(b); return b; }
};
