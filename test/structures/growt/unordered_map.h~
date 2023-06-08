#include <atomic>
#include <random>
#include <thread>

#include "allocator/alignedallocator.hpp"
#include "data-structures/hash_table_mods.hpp"
#include "utils/hash/murmur2_hash.hpp"

using hasher_type    = utils_tm::hash_tm::murmur2_hash;
using allocator_type = growt::AlignedAllocator<>;
#include "data-structures/table_config.hpp"

using KV_Type = unsigned long;
using table_type =
  typename growt::table_config<KV_Type, KV_Type, hasher_type, allocator_type,
			       hmod::growable, hmod::deletion>::table_type;

template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {

  using handle_type = typename table_type::handle_type;
  static std::vector<handle_type*> handle_pointers;
  table_type table;

  handle_type* get_handle() {
    return handle_pointers[parlay::worker_id()];
  }

  std::optional<V> find(const K& k) {
    handle_type* my_handle = get_handle();
    auto r = my_handle->find(k);
    if (r == my_handle->end()) return std::optional<V>();
    else return (*r).second;
  }

  bool insert(const K& k, const V& v) {
    handle_type* my_handle = get_handle();
    auto x = my_handle->insert(k, v);
    return x.second;
  }

  bool remove(const K& k) {
    handle_type* my_handle = get_handle();
    return my_handle->erase(k);
  }

  unordered_map(size_t n) : table(table_type(n)) {
    for (int i=0; i< parlay::num_workers(); i++)
      handles[i] = new handle_type(table.get_handle());
  }

  long size() {
    handle_type* my_handle = get_handle();
    long count;
    for (auto it = my_handle->begin(); it != my_handle->end(); it++) count++;
    return count;}
};

template <typename K, typename V, typename H, typename E>
std::vector<table_type::handle_pointers*> unordered_map<K,V,H,E>::handle_pointers;
