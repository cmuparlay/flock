#pragma once
using IT = size_t;

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
  write_once<persistent*> next_version;
  static constexpr size_t init_ptr =(1ul << 48) - 2;
  persistent* add_tag(persistent* v, bool tag) {
    return (persistent*) ((size_t) v + tag);}
  bool is_indirect() {return (IT) next_version.load() & 1;}
  persistent* get_next() {return (persistent*) ((IT) next_version.load() & ~1ul);}
  TS read_stamp() {return time_stamp.load();}
  TS load_stamp() {return commit(time_stamp.load());}
  void set_stamp(TS t) {
    TS old = tbd;
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

  #ifdef alwaysIndirect
  static V* set_zero_stamp(V* ptr) {
    V* new_v = (V*) link_pool.new_obj((persistent*) nullptr, ptr);
    new_v->time_stamp = zero_stamp;
    return new_v;
  }
#else
  static V* set_zero_stamp(V* ptr) {
    if (ptr != nullptr && ptr->read_stamp() == tbd)
      ptr->time_stamp = zero_stamp;
    return ptr;
  }
#endif

  V* shortcut(plink* ptr) {
    if (ptr->read_stamp() <= done_stamp)
      if (v.single_cas((V*) ptr, (V*) pptr->value))
#ifdef NoHelp
      	  link_pool.retire(pptr);
#else
      link_pool.pool.retire(pptr);
#endif
      return (V*) pptr->value;
    }
    return ptr;
  }

#ifdef alwaysIndirect
  V* get_ptr(V* ptr) { return ((plink*) ptr)->value;}
#elseif NoShortcut
V* get_ptr(V* ptr) {
  if (ptr != nullptr && ptr->is_indirect()) 
    return (plink*) ptr->value;
  else return ptr;
}
#else
V* get_ptr(V* ptr) {
  if (ptr != nullptr && ptr->is_indirect()) 
    return shortcut((plink*) ptr->value);
  else return ptr;
}
#endif


public:

  persistent_ptr(): v(nullptr) {}
  persistent_ptr(V* ptr) : v(set_zero_stamp(ptr)) {}
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

#ifdef Indirect
  void store(V* ptr) {
    V* old_v = v.load();
    V* new_v = (V*) link_pool.new_obj((persistent*) old_v, ptr);
    v.cam(old_v, new_v);
    link_pool.retire((plink*) old_v);    
    set_stamp(new_v);
  }
#elseif NoShortcut
  void store(V* ptr) {
    V* old_v = v.load();
    V* new_v = ptr;

    if (ptr == nullptr || ptr->load_stamp() != tbd)
      new_v = (V*) link_pool.new_obj((persistent*) old_v, ptr);
    else ptr->next_version = old_v;

    v.cam(old_v, new_v);
    
    if (old_v != nullptr && old_v->is_indirect())
      link_pool.retire((plink*) old_v);
    
    set_stamp(new_v);
  }
#else
  void store(V* ptr) {
    V* old_v = v.load();
    V* new_v = ptr;
    bool use_indirect = (ptr == nullptr || ptr->load_stamp() != tbd);

    if (use_indirect)
      new_v = (V*) link_pool.new_obj((persistent*) old_v, ptr);
    else ptr->next_version = old_v;

    v.cam(old_v, new_v);
    
    if (old_v != nullptr && old_v->is_indirect()) {
      V* val = v.load();
      if (val != (V*) ((plink*) old_v)->value)
	link_pool.retire((plink*) old_v);
      else v.cam(val, new_v);
    }
    
    set_stamp(new_v);
    if (use_indirect) shortcut((plink*) new_v);
  }
#endif
  V* operator=(V* b) {store(b); return b; }
};
