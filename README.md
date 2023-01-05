
# Flock : A library for lock-free locks

Flock is a C++ library supporting lock-free locks as described in the paper:

Lock-free Locks: Revisited \
Naama Ben-David, Guy E. Blelloch, Yuanhao Wei \
PPoPP 2022

To access the artifact evaluation for that paper, go to the "ae" branch.
The version described here differs in some of the function names.

## Library functionality

The library supplies a variety of components.  It supplies a `flck::lock`
type with a `try_lock` method.  The `try_lock` takes as an argument a
thunk (C++ lambda with no arguments) returning a boolean.

Mutable shared values accessed inside of a lock need to be wrapped in a `flck::atomic<Type>`.
The interface for a `flck::atomic` is similar to `std::atomic` and in particular supplies the
interface:

```
template <typename T>
struct flck::atomic<T> {
  flck::atomic(T v);      // constructor
  T load();               // load contents
  void store(T val);      // store contents
  cam(T expected, T new); // compare and modify
  T operator=(T val);     // assignment
}
```

The `cam` is similar to `compare_exchange_strong` for c++ `std`
atomics, but it has no return value and does not side effect `expected`.

A simple example is:

```
flck::lock lck;
flck::atomic<int> a = 1;
flck::atomic<int> b = 2;

auto swap = [=] {
  int tmp = a.load();
  a = b.load();
  b = tmp;
  return true;};

lck->try_lock(swap);
  < in parallel with >
lck->try_lock(swap);
```

Because of the lock, this ensures that the two swaps are not
interleaved.  Hence the final contents are either swapped if one `try_lock`
fails or the same as initially if both succeed.  A `try_lock` will
only fail and not run its code if the lock is taken, so at least one
will succeed. The return value of the `try_lock` is false if it fails,
and otherwise is the return value of the lambda (in this case true).

A `try_lock`  can be nested to acquire multiple locks, as in:

```
flck::lock lck1, lck2;
flck::atomic<bool> a = false;

bool ok = lck1->try_lock([=] {
   return lck2->try_lock([=] {
      a = true;
      return true;
   });
});

assert(ok == a);
```
Note that `ok` will be true iff both try locks succeed.

Memory for objects allocated or freed in a lock must be managed
through the flock epoch-based memory manager.   It supplies the structure:

```
template <typename T>
struct flck::memory_pool<T> {
  template <typename ... Args>
  T* new_obj(Args... args);   // allocate
  void retire(T* ptr);      // free
}
```

The `new_obj` method will allocate a new object of type `T` passing
`args` to its constructor.  The `retire` will reclaim the memory,
although this might be delayed due to the epoch based collection.
Also all concurrent operations should be wrapped in
`flck::with_epoch(thunk)`.  For example:

```
struct link : flck::lock {
  flck::atomic<list*> next;
  int val;
  link(list* next, int val) : next(next), val(val);
}

flck::memory_pool<link> links;

bool add_after(link l, int val) {
  return with_epoch([=] {
    return l->try_lock([=] {
      l->next = links.new_obj(l->next, val);
      return true;
    });
  });
}
```

In addition to the `flck::atomic<T>` structure flock provides the `flck::atomic_write_once` structure.   It has the same interface, but its value can only be written once after initialization.  This can improve performance in some cases.

By default `try_lock` are lock free due to the helping mechanism.
They can also be used as blocking try locks by setting the compiler
flag `NoHelp`.

## Making and Directory Structure

Flock is a header only library and uses cmake by default (although one
can use any building tool).  The directory structure is as follows:

```
- flock
  - CMakeLists.txt
  - include
    - flock
      - flock.h    // this needs to be included 
      ...          // various .h files used internally by flock
  - structures         // the flock data structures
    - arttree
      - set.h
    - [avltree blockleaftree btree dlist hash hash_block leaftree list list_onelock]
  - benchmark
    - CMakeLists.txt
    - test_sets.cpp   // the benchmarking driver
    - runtests        // a script that runs various tests
    - [ various .h files]
  - setbench         // code from Trevor Brown's setbench adapted to work with flock benchmarks
    - CMakeLists.txt
    - ...
```

You can get started by cloning the library, and then:

```
cd flock
mkdir build
cd build
cmake -DDOWNLOAD_PARLAY ..
cd benchmarks
make -j
./runtests -test
```

The `-DDOWNLOAD_PARLAY` is not needed if you have `parlaylib` installed.

## Example

Here is a full example of an implementation of doubly linked list.  It
also appears in `structures/dlist/set.h`.

```
  struct node : flck::lock {
    bool is_end;
    flck::atomic_write_once<bool> removed;
    flck::atomic<node*> prev;
    flck::atomic<node*> next;
    K key;
    V value;
    node(K key, V value, node* next, node* prev)
      : key(key), value(value), is_end(false), removed(false),
      next(next), prev(prev) {};
  };

  flck::memory_pool<node> nodes;

  auto find_location(node* root, K k) {
    node* nxt = (root->next).load();
    while (true) {
      node* nxt_nxt = (nxt->next).load(); 
      if (nxt->is_end || nxt->key >= k) break;
      nxt = nxt_nxt;
    }
    return nxt;
  }

  std::optional<V> find(node* root, K k) {
    return flck::with_epoch([=] { 
      node* loc = find_location(root, k);
      if (!loc->is_end && loc->key == k) return loc->value; 
      else return {};
    });
  }

  bool insert(node* root, K k, V v) {
    return flck::with_epoch([&] {
      while (true) {
        node* next = find_location(root, k);
        if (!next->is_end && next->key == k) return false;
        node* prev = (next->prev).load();
        if ((prev->is_end || prev->key < k) && 
            prev->try_lock([=] {
              if (!prev->removed.load() &&
	          (prev->next).load() == next) {
                auto new_node = nodes.new_obj(k, v, next, prev);
                prev->next = new_node;
                next->prev = new_node;
                return true;
              } else return false;}))
          return true;
      }
    });
  }

  bool remove(node* root, K k) {
    return flck::with_epoch([&] {
      while (true) {
        node* loc = find_location(root, k);
        if (loc->is_end || loc->key != k) return false;
        node* prev = (loc->prev).load();
        if (prev->try_lock([=] {
              if (prev->removed.load() ||
                  (prev->next).load() != loc)
                return false;
              return loc->try_lock([=] {
                  node* next = (loc->next).load();
                  loc->removed = true;
                  prev->next = next;
                  next->prev = prev;
                  nodes.retire(loc);
                  return true;
                });}))
          return true;
      }
    });
  }
```
