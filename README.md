
# Lock-free Locks: Revisited

This artifact contains the source code and scripts to reproduce the graphs in the following paper:

Lock-free Locks: Revisited \
Naama Ben-David, Guy E. Blelloch, Yuanhao Wei \
PPoPP 2022

## Hardware/Software Requirements
  - A multicore machine (preferably with 64+ cores)
  - compiler: g++-7 or higher
  - jemalloc-5.2.1: https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2
    - To install, extract and follow instructions in the INSTALL text file
    - Then run ```sudo apt install libjemalloc-dev```
  - python3 (we use version 3.8.10) with matplotlib (we use version 3.1.2)
    - sudo apt-get install python3-matplotlib

## Compiling and running tests

```
    bash compile_all.sh   # compiles everything
    python3 run_tests.py  # runs tests
```

  - Expected output for ```run_tests.py``` can be found in ```run_tests_expected_output.txt```

## Running experiments and generating graphs
  - Note: these steps assume ```bash compile_all.sh``` has already been executed
  - To reproduce all the graphs in the paper, run ```bash generate_graphs_from_paper.sh```
    - this command will take ~5 hours to run
  - The output graphs will be stored in the graphs/ directory
  - You can rerun a specific graph by running the corresponding command from the 
    generate_graphs_from_paper.sh file. Each command generates a single graph.
  - You can also run custom experiments (and generate graphs for them) using the following script: 

```
    python3 run_experiments.py [datastructures] [threads] [sizes] [zipfians] [ratios]
```
  - See generate_graphs_from_paper.sh for examples of how to use the above script
  - Parameter description: 
    - datastructures: type of data structures to run, pick one of [lists, trees, sets, with-vs-try-lock]
    - threads: number of threads
    - sizes: initial size
    - zipfians: Zipfian parameter, number between [0, 1)
    - ratios: percentage of updates, number between 0 and 100
    - Exactly one of the parameters [threads, size, zipfians, ratios] has to be a list of numbers. This parameter will be used as the x-axis of the generated graph.