#pragma once
using IT = size_t;

#ifdef NoHelp
#include "no_tagged.h"
template <typename T>
using tagged = no_tagged<T>;
template <typename F>
bool skip_if_done(F f) {f(); return true;}
IT commit(IT v) {return v;}

#else // Helping
#include "lf_log.h"
IT commit(IT v) {return lg.commit_value(v).first;}
#endif

parlay::sequence<long> i_counts(parlay::num_workers()*16, 0);
void print_counts() {
  std::cout << " indirect = " << parlay::reduce(i_counts) << std::endl;
}

#define bad_ptr ((1ul << 48) -1)

using IT = size_t;
struct persistent {
  std::atomic<TS> time_stamp;
  std::atomic<IT> next_version;
  persistent() : time_stamp(tbd), next_version(bad_ptr) {}
  persistent(IT next) : time_stamp(tbd), next_version(next) {}
};

struct plink : persistent {
  IT value;
  plink(IT next, IT value) : persistent(next), value(value) {}  
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
  static V* remove_unset(V* ptr) {return (V*) (~4ul & (IT) ptr);};
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

  static V* set_zero(V* ptr) {
    if (ptr != nullptr && ptr->time_stamp.load() == tbd)
      ptr->time_stamp = zero_stamp;
    return ptr;
  }

  // For an indirect pointer if its stamp is older than done_stamp
  // then it will no longer be accessed and can be spliced out.
  // The splice needs to be done under a lock since it can race with updates
  std::pair<V*,bool> shortcut_indirect(IT ptr) {
    auto ptr_notag = (plink*) strip_mark_and_tag(ptr);
    if (is_indirect(ptr)) {
      //i_counts[16*parlay::worker_id()]++;
      TS stamp = ptr_notag->time_stamp.load();
      V* newv = is_empty(ptr) ? nullptr : (V*) ptr_notag->value;
      if (stamp <= done_stamp) {
      	// there can't be an ABA unless indirect node is reclaimed
        if (TV::cas_with_same_tag(v, ptr, newv, true)) 
      	  link_pool.retire(ptr_notag);
	return std::pair{newv, true};
      } else return std::pair{newv, false};
    } else return std::pair{(V*) ptr_notag, false};
  }

public:

  persistent_ptr(V* v) : v(TV::init(set_zero(v))) {}
  persistent_ptr(): v(TV::init(0)) {}
  ~persistent_ptr() {
    IT ptr = v.load();
    if (is_indirect(ptr))
      link_pool.destruct((plink*) strip_mark_and_tag(ptr));
  }

  void init(V* vv) {v = TV::init(set_zero(vv));}
  // reads snapshotted version
  V* read_snapshot() {
    TS ls = local_stamp;
    IT head = v.load();
    set_stamp(head);
    V* head_unmarked = strip_mark_and_tag(head);
    // chase down version chain
    while (head_unmarked != 0 && head_unmarked->time_stamp.load() > ls) {
      head = head_unmarked->next_version.load();
      head_unmarked = strip_mark_and_tag(head);
    }
    return get_ptr(head);
  }

  V* load() {  // can be used anywhere
    if (local_stamp != -1) return read_snapshot();
    else return shortcut_indirect(set_stamp(commit(v.load()))).first;}

  V* read() {  // only safe on journey
    //if (local_stamp != -1) read_snapshot();
    return shortcut_indirect(v.load()).first;
  }

  V* read_cur() {  // only safe on journey, outside of snapshot
    return shortcut_indirect(v.load()).first;
  }

  void validate() {
    set_stamp(v.load());     // ensure time stamp is set
  }
  
  void store(V* newv_) {
    IT oldv_tagged = commit(v.load());
    V* oldv = strip_mark_and_tag(oldv_tagged);
    V* newv = newv_;
    V* newv_marked = newv;

    skip_if_done([&] {
    if (newv_ == nullptr || commit(newv_->time_stamp.load() != tbd)) {
      newv = (V*) link_pool.new_obj((IT) oldv_tagged, (IT) newv);
      newv_marked = ((newv_ == nullptr)
		     ? add_null_mark(newv)
		     : add_indirect_mark(newv));
    } else {
      IT initial_ptr = bad_ptr;
      if(newv->next_version == initial_ptr)
	newv->next_version.compare_exchange_strong(initial_ptr,
						   (IT) oldv_tagged);
    }
    // swap in new pointer but marked as "unset" since time stamp is tbd
    V* newv_unset = add_unset(newv_marked);
    bool succeeded = TV::cas(v, oldv_tagged, newv_unset);
    IT x = commit(v.load());
    
    if (is_indirect(oldv_tagged)) {
      if (succeeded) link_pool.retire((plink*) oldv);
      else if (TV::get_tag(x) == TV::get_tag(oldv_tagged)) { 
	succeeded = TV::cas(v, x, newv_unset);
 	x = commit(v.load());
      }
    } 

    // now set the stamp from tbd to a real stamp
    set_stamp(x);

    // try to shortcut indirection out, and if not, clear unset mark
    // for time stamp
    if (!shortcut_indirect(x).second)
      TV::cas(v, x, remove_unset(TV::value(x)));  // clear the "unset" mark

    // shortcut version list if appropriate, getting rid of redundant
    // time stamps.  
    if (oldv != nullptr && newv->time_stamp == oldv->time_stamp) 
      newv->next_version = oldv->next_version.load();

    // free if allocated link was not used
    if (!succeeded && is_indirect((IT) newv_marked))
      link_pool.destruct((plink*) newv);
    		 });
  }
  
  V* operator=(V* b) {store(b); return b; }
};
