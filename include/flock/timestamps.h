#pragma once
#include <limits>
#include <atomic>
// code for timestamps for snapshots

void inc_backoff(int &bk, int max) {if (bk < max) bk = round(1.1*bk);}
void dec_backoff(int &bk) {if (bk > 1) bk = round(.9*bk);}

using TS = long;

struct timestamp_simple {
  std::atomic<TS> stamp;
  static constexpr int delay = 800;

  TS get_stamp() {return stamp.load();}
      
  TS get_read_stamp() {
    TS ts = stamp.load();

    // delay to reduce contention
    for(volatile int i = 0; i < delay; i++);

    // only update timestamp if has not changed
    if (stamp.load() == ts)
      stamp.compare_exchange_strong(ts,ts+1);

    return ts;
  }

  TS get_write_stamp() {return stamp.load();}
  timestamp_simple() : stamp(1) {}
};

struct timestamp_simple_update {
  std::atomic<TS> stamp;
  static constexpr int delay = 800;

  TS get_read_stamp() {return stamp.load();}

  TS get_write_stamp() {
    TS ts = stamp.load();

    // delay to reduce contention
    for(volatile int i = 0; i < delay; i++);

    // only update timestamp if has not changed
    if (stamp.load() == ts)
      stamp.compare_exchange_strong(ts,ts+1);

    return ts+1;
  }
  timestamp_simple_update() : stamp(1) {}
};

struct timestamp_multiple {
  static constexpr int slots = 4;
  static constexpr int gap = 16;
  static constexpr int delay = 300;
  std::atomic<TS> stamps[slots*gap];

  inline TS get_write_stamp() {
    TS total = 0;
    for (int i = 0; i < slots; i++)
      total += stamps[i*gap].load();
    return total;
  }

  TS get_read_stamp() {
    TS ts = get_write_stamp();
    //int i = sched_getcpu() / 36; //% slots;
    int i = parlay::worker_id() % slots;
    for(volatile int j = 0; j < delay ; j++);
    TS tsl = stamps[i*gap].load();
    if (ts == get_write_stamp()) {
      //int i = parlay::worker_id() % slots;
      stamps[i*gap].compare_exchange_strong(tsl,tsl+1);
    }
    return ts;
  }

  timestamp_multiple() {
    for (int i = 0; i < slots; i++) stamps[i*gap] = 1;
  }
};


// works well if mostly reads or writes
// if stamp is odd then in write mode, and if even in read mode
// if not in the right mode, then increment to put in the right mode
thread_local float read_backoff = 50.0;
thread_local float write_backoff = 1000.0;
struct timestamp_read_write {
  std::atomic<TS> stamp;
  // unfortunately quite sensitive to the delays
  // these were picked empirically for a particular machine (aware)
  //static constexpr int write_delay = 1500;
  //static constexpr int read_delay = 150;

  TS get_stamp() {return stamp.load();}

  inline TS get_write_stamp() {
    TS s = stamp.load();
    if (s % 2 == 1) return s;
    //for(volatile int j = 0; j < write_delay ; j++);
    for(volatile int j = 0; j < round(write_backoff) ; j++);
    if (s != stamp.load()) return s;
    if (stamp.compare_exchange_strong(s,s+1)) {
      if (write_backoff > 1200.0) write_backoff *= .98;
    } else if (write_backoff < 1800.0) write_backoff *= 1.02;
    return s+1; // return new stamp
  }

  TS get_read_stamp() {
    TS s = stamp.load();
    if (s % 2 == 0) return s;
    for(volatile int j = 0; j < round(read_backoff) ; j++);
    if (s != stamp.load()) return s;
    if (stamp.compare_exchange_strong(s,s+1)) {
      if (read_backoff > 10.0) read_backoff *= .98;
    } else if (read_backoff < 400.0) read_backoff *= 1.02;
    return s; // return old stamp
  }

  TS current() { return stamp.load();}
  
  timestamp_read_write() : stamp(1) {}
};

//timestamp_simple global_stamp;
timestamp_read_write global_stamp;
const TS tbd = std::numeric_limits<TS>::max();
thread_local TS local_stamp{-1};

// this is updated by the epoch-based reclamation
// Whenever an epoch is incremented this is set to the stamp
// from the previous increment (which is now safe to collect).
TS done_stamp = global_stamp.get_stamp();

template <typename F>
auto with_snapshot(F f) {
  return with_epoch([&] {
    local_stamp = global_stamp.get_read_stamp();
    auto r = f();
    local_stamp = -1;
    return r;
  });
}

