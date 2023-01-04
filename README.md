
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
flck::atomic<int> a;

lck1->try_lock([=] {
   return lck2->try_lock([=] {
      a = 1;
      return true;
   });
});
```

Memory for objects allocated or freed in a lock must be managed
through the flock epoch-based memory manager.   It supplies the structure:

```
template <typename T>
struct flck::memory_pool<T> {
  template <typename ... Args>
  T* alloc(Args... args);   // allocate
  void retire(T* ptr);      // free
}
```

The `alloc` method will allocate a new object of type `T` passing
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
    l->try_lock([=] {
      l->next = links.alloc(l->next, val);
    });
  });
}
```

## Making and Directory Structure

Flock uses cmake.   The directory structure is as follows:

```
- flock
  - CMakeLists.txt
  - include
    - flock
      - flock.h    // this needs to be included 
      ...
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
