#include <atomic>
#include <random>
#include <thread>

#include "allocator/alignedallocator.hpp"
#include "data-structures/hash_table_mods.hpp"
#include "utils/hash/murmur2_hash.hpp"

#include "data-structures/table_config.hpp"
template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {

  using K_Type = unsigned long;
  using V_Type = unsigned long;
  using hasher_type    = utils_tm::hash_tm::murmur2_hash;
  using allocator_type = growt::AlignedAllocator<>;

  using table_type =
    typename growt::table_config<K_Type, V_Type, hasher_type, allocator_type,
				 hmod::growable, hmod::deletion>::table_type;

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
    int p = parlay::num_workers();
    handle_pointers = std::vector<handle_type*>(p);
    for (int i=0; i < p; i++)
      handle_pointers[i] = new handle_type(table.get_handle());
  }

  ~unordered_map() {
    int p = parlay::num_workers();
    for (int i=0; i < p; i++)
      delete handle_pointers[i]; 
  }

  long size() {
    handle_type* my_handle = get_handle();
    long count = 0;
    for (auto x : *my_handle) count++;
    return count;}
};

template <typename K, typename V, typename H, typename E>
std::vector<typename unordered_map<K,V,H,E>::handle_type*> unordered_map<K,V,H,E>::handle_pointers;
