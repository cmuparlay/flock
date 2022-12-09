# Re-implementation of Lock Free Contention Adapting Search Trees

## Building and Running

This project is built with CMake version 3.0.0+ using g++ with support for C++11 and posix threads. Ensure the correct versions of `cmake` and `g++` are installed and added to the system path before continuing.

1. Navigate to the `build/` directory in the terminal.
2. Run `cmake ..` to configure CMake. This only needs to be done once.
    - Note: If the build fails or the wrong compiler is used, the generator may need to be specified. To do this, add `-G "<generator name>"` to the command. On Windows using MinGW, this would be `cmake -G "MinGW Makefiles" ..`. For a full list of generators, run `cmake --help`. Before running the command again, delete the contents of the `build` folder.
2. Run `cmake --build .` to build the project. Do not forget the dot.
3.  Run `./lfca` to execute the test program, or `./TEST` To execute the unit tests
