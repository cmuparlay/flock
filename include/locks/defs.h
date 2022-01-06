#pragma once

constexpr bool debug = false;
bool wait_before_retrying_lock = false;

// if set to false will use normal spin locks in place of helping locks
bool use_help = true;
bool try_only = true;

// a flag to indicate that currently helping
static thread_local bool helping = false;

// deprecated
static thread_local bool help = true;

bool verbose = false;
