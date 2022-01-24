
# Flock : A library for lock-free locks

Flock is a C++ library supporting lock-free locks as described in the paper:

Lock-free Locks: Revisited \
Naama Ben-David, Guy E. Blelloch, Yuanhao Wei \
PPoPP 2022

To access the artifact evaluation for that paper, go to the "ae" branch.

# Library functionality

The library supplies a variety of components.  It supplies a `lock`
type and a `try_lock` function.  The `try_lock` takes as arguments a
lock, and a thunk (C++ lambda with no arguments, returning a boolean).

Mutable values accessed inside of a lock need to be wrapped in a `mutable_<Type>`.
A simple example is:

```
lock lck;
mutable_<int> a, b;

try_lock(lck, [=] {a = 1; b = 1; return true;});
  < in parallel with >
try_lock(lck, [=] {a = 2; b = 2; return true;});
```

Because of the lock, this ensures they are `a` and `b` are both 1 or
both 2.  A `try_lock` will only fail and not run its code if the lock
is taken.  The return value of the `try_lock` is false if it fails,
and otherwise is the return value of the lambda.

The mutable type has a similar interface as C++ atomics,
supporting `load()`, `store(x)` and `cam(oldv, newv)`.  Assignment is
overloaded to do a store (as in the example above).  The `cam`
(compare and modify) is similar to `compare_exchange_strong` for c++
atomics, but it has no return value and does not side effect `oldv`.

The `try_lock` function can be nested to acquire multiple locks, as in:

```
lock lck1, lck2;
mutable_<int> a;

try_lock(lck1, [=] {
   return try_lock(lck2, [=] {
      a = 1;
      return true;
   });
});
```


