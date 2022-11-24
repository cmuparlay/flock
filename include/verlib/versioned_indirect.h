#pragma once
#include "flock/flock.h"
#include "timestamps.h"

namespace vl {
struct versioned {};

struct version_link {
  std::atomic<TS> time_stamp;
  version_link* next_version;
  void* value;
  version_link() : time_stamp(tbd) {}
  version_link(TS time, version_link* next, void* value) :
    time_stamp(time), next_version(next), value(value) {}  
};

flck::memory_pool<version_link> link_pool;

template <typename V>
struct versioned_ptr {
private:
  flck::atomic<version_link*> v;

  static version_link* set_stamp(version_link* ptr) {
    if (ptr->time_stamp.load() == tbd) {
      TS old_t = tbd;
      TS new_t = global_stamp.get_write_stamp();
      if (ptr->time_stamp.load() == tbd)
        ptr->time_stamp.compare_exchange_strong(old_t, new_t);
    }
    return ptr;
  }

  static version_link* init_ptr(V* ptr) {
    return link_pool.new_obj(zero_stamp, nullptr, (void*) ptr);
  }

public:

  versioned_ptr(): v(init_ptr(nullptr)) {}
  versioned_ptr(V* ptr) : v(init_ptr(ptr)) {}
  ~versioned_ptr() { link_pool.destruct(v.load()); }
  void init(V* ptr) {v = init_ptr(ptr);}
  
  V* read_snapshot() {
    version_link* head = set_stamp(v.load());
    while (head->time_stamp.load() > local_stamp)
      head = head->next_version;
#ifdef LazyStamp
    if (head->time_stamp.load() == local_stamp && speculative)
      aborted = true;
#endif
    return (V*) head->value;
  }

  V* load() {  // can be used anywhere
    if (local_stamp != -1) return read_snapshot();
    else return (V*) set_stamp(v.load())->value;
  }

  // only safe on journey
  V* read() {  return (V*) v.read()->value; }

  void validate() { set_stamp(v.load()); }

  void store(V* ptr) {
    version_link* old_link = v.load();
    version_link* new_link = link_pool.new_obj(tbd, old_link, (void*) ptr);
    v = new_link;
    set_stamp(new_link);
    link_pool.retire(old_link);    
  }

  bool cas(V* old_v, V* new_v) {
    version_link* old_link = v.load();
    if(old_link != nullptr) set_stamp(old_link);
    if (old_v != old_link->value) return false;
    if (old_v == new_v) return true;
    version_link* new_link = link_pool.new_obj(tbd, old_link, (void*) new_v);
    if (v.cas_ni(old_link, new_link)) {
      set_stamp(new_link);
      link_pool.retire(old_link);
      return true;
    }
    set_stamp(v.load());
    link_pool.destruct(new_link);
    return false;
  }

  V* operator=(V* b) {store(b); return b; }
};

} // namespace vl
