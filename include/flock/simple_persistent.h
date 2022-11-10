#pragma once
using IT = size_t;
// #define NoShortcut 1

#ifdef NoHelp
template <typename F>
bool skip_if_done(F f) {f(); return true;}
IT commit(TS v) {return v;}
#else // Helping
TS commit(TS v) {return lg.commit_value(v).first;}
#endif

parlay::sequence<long> i_counts(parlay::num_workers()*16, 0);
void print_counts() {
  std::cout << " indirect = " << parlay::reduce(i_counts) << std::endl;
}

struct persistent {
  size_t foo;
  std::atomic<TS> time_stamp;
  persistent* next_version;
  static constexpr size_t init_ptr =(1ul << 48) - 2;
  persistent* add_tag(persistent* v, bool tag) {
    return (persistent*) ((size_t) v + tag);}
  bool is_indirect() {return (IT) next_version & 1;}
  persistent* get_next() {return (persistent*) ((IT) next_version & ~1ul);}
  TS read_stamp() {return time_stamp.load();}
  TS load_stamp() {return commit(time_stamp.load());}
  void set_stamp(TS t) {
    TS old = tbd;
    if(time_stamp.load() == tbd)
      time_stamp.compare_exchange_strong(old, t);
  }
  persistent() : time_stamp(tbd), next_version((persistent*) init_ptr) {}
  persistent(persistent* next, bool is_indirect)
    : time_stamp(tbd), next_version(add_tag(next,is_indirect)) {}
};

struct plink : persistent {
  void* value;
  plink(persistent* next, void* value) : persistent{next, true}, value(value) {}  
};

memory_pool<plink> link_pool;

template <typename V>
struct persistent_ptr {
private:
  mutable_val<V*> v;

  static V* set_stamp(V* ptr) {
    if (ptr != nullptr && ptr->read_stamp() == tbd)
      ptr->set_stamp(global_stamp.get_write_stamp());
    return ptr;
  }

  static V* set_zero_stamp(V* ptr) {
    if (ptr != nullptr && ptr->read_stamp() == tbd)
      ptr->time_stamp = zero_stamp;
    return ptr;
  }

  void shortcut(plink* ptr) {
      if (ptr->read_stamp() <= done_stamp)
        if (v.single_cas((V*) ptr, (V*) ptr->value))
#ifdef NoHelp
      	  link_pool.retire(ptr);
#else
      link_pool.pool.retire(ptr);
#endif
  }

  V* get_ptr(V* ptr) {
    if (ptr != nullptr && ptr->is_indirect()) {
#ifndef NoShortcut
      shortcut((plink*) ptr);
#endif
      return (V*) ((plink*) ptr)->value;
    } else return ptr;
  }

public:

  persistent_ptr(): v(0) {}
  persistent_ptr(V* ptr) : v(set_zero_stamp(ptr)) {}

  ~persistent_ptr() {
    plink* ptr = (plink*) v.read();
    if (ptr != nullptr && ptr->is_indirect())
      link_pool.pool.destruct_no_log(ptr);
  }

  void init(V* ptr) {v = set_zero_stamp(ptr);}

  V* read_snapshot() {
    TS ls = local_stamp;
    V* head = v.load();
    set_stamp(head);
    while (head != nullptr && head->read_stamp() > ls)
      head = (V*) head->get_next();
    return ((head != nullptr) && head->is_indirect()) ? (V*) ((plink*) head)->value : head;
  }

  V* load() {  // can be used anywhere
    if (local_stamp != -1) return read_snapshot();
    else return get_ptr(set_stamp(v.load()));
  }
  
  V* read() {  // only safe on journey
    return get_ptr(v.read());
  }

  V* read_cur() {  // only safe on journey, outside of snapshot
    return get_ptr(v.read());
  }

  void validate() {
    set_stamp(v.load());     // ensure time stamp is set
  }
  
  void store(V* ptr) {
    V* old_v = v.load();
    V* new_v = ptr;
    bool use_indirect = (ptr == nullptr || ptr->load_stamp() != tbd);

    if (use_indirect)
      new_v = (V*) link_pool.new_obj((persistent*) old_v, ptr);
    else ptr->next_version = old_v;

#ifdef NoShortcut
    v = new_v;
    if (old_v != nullptr && old_v->is_indirect()) 
      link_pool.retire((plink*) old_v);
#else
    v.cam(old_v, new_v);
    if (old_v != nullptr && old_v->is_indirect()) {
      // link_pool.retire((plink*) old_v);
      V* val = v.load();
      if (val != (V*) ((plink*) old_v)->value)
	      link_pool.retire((plink*) old_v);
      else v.cam(val, new_v);
    }
#endif
    set_stamp(new_v);
#ifndef NoShortcut
    if (use_indirect) shortcut((plink*) new_v);
#endif
  }
  
  bool cas(V* expv, V* newv) {
#ifndef NoShortcut
    for(int ii = 0; ii < 2; ii++) {
#endif
      V* new_v = newv;
      V* oldv = v.load();
      if(oldv != nullptr) set_stamp(oldv);
      if(get_ptr(oldv) != expv) return false;
      if(oldv == newv) return true;
      bool use_indirect = (newv == nullptr || newv->load_stamp() != tbd);

      if(use_indirect)
        new_v = (V*) link_pool.new_obj((persistent*) oldv, newv);
      else newv->next_version = oldv;

      bool succeeded = v.single_cas(oldv, new_v);

      if(succeeded) {
        set_stamp(new_v);
        if(oldv != nullptr && oldv->is_indirect()) 
          link_pool.retire((plink*) oldv);
#ifndef NoShortcut
          if (use_indirect) shortcut((plink*) new_v);
#endif
        return true;
      }
      if(use_indirect) link_pool.destruct((plink*) new_v);

#ifndef NoShortcut
      // only repeat if oldv was indirect
      // if(ii == 0 && (oldv == nullptr || !oldv->is_indirect())) break;

    }
#endif
    V* curv = v.load();
    if(curv != nullptr) set_stamp(curv);
    return false;
  }

  V* operator=(V* b) {store(b); return b; }
};
