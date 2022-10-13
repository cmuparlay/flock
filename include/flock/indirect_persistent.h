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

struct persistent {};

struct plink {
  std::atomic<TS> time_stamp;
  write_once<plink*> next_version;
  void* value;
  plink() : time_stamp(tbd) {}
  plink(TS time, plink* next, void* value) :
    time_stamp(time), next_version(next), value(value) {}  
};

memory_pool<plink> link_pool;

template <typename V>
struct persistent_ptr {
private:
  mutable_val<plink*> v;

  static plink* set_stamp(plink* ptr) {
    if (ptr->time_stamp.load() == tbd) {
      TS old_t = tbd;
      TS new_t = global_stamp.get_write_stamp();
      ptr->time_stamp.compare_exchange_strong(old_t, new_t);
    }
    return ptr;
  }

  static plink* init_ptr(V* ptr) {
    return link_pool.new_obj(zero_stamp, nullptr, (void*) ptr);
  }

  V* get_ptr(plink* ptr) { return (V*) ptr->value;}

public:

  persistent_ptr(): v(nullptr) {}
  persistent_ptr(V* ptr) : v(init_ptr(ptr)) {}
  void init(V* ptr) {v = init_ptr(ptr);}
  V* read_snapshot() {
    TS ls = local_stamp;
    plink* head = set_stamp(v.load());
    while (head != nullptr && head->time_stamp.load() > ls)
      head = head->next_version.load();
    return (V*) head->value;
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
    plink* old_v = v.load();
    plink* new_v = link_pool.new_obj(tbd, old_v, (void*) ptr);
    v.cam(old_v, new_v);
    link_pool.retire(old_v);    
    set_stamp(new_v);
  }
  V* operator=(V* b) {store(b); return b; }
};
