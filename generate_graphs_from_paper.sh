

# Figure 4
python3 run_experiments.py with-vs-try-lock 144 100000 [0,0.75,0.9,0.99] 50

# Figure 5a
python3 run_experiments.py trees [1,72,144,216,288] 100000000 0.75 50

# Figure 5b
python3 run_experiments.py trees 144 100000000 0.75 [0,5,10,50]

# Figure 5c
python3 run_experiments.py trees 144 100000000 [0,0.75,0.9,0.99] 50

# Figure 5d
python3 run_experiments.py trees 216 100000000 [0,0.75,0.9,0.99] 50

# Figure 5e
python3 run_experiments.py trees [1,72,144,216,288] 100000 0.75 50

# Figure 5f
python3 run_experiments.py trees 144 100000 0.75 [0,5,10,50]

# Figure 5g
python3 run_experiments.py trees 216 100000 [0,0.75,0.9,0.99] 5

# Figure 5h
python3 run_experiments.py trees 216 [1000,100000,10000000,100000000] 0.75 5

# Figure 6a
python3 run_experiments.py sets [1,72,144,216,288] 100000000 0.75 50

# Figure 6b
python3 run_experiments.py sets 216 100000000 [0,0.75,0.9,0.99] 5

# Figure 8a
python3 run_experiments.py lists 144 [10,100,1000,10000] 0.75 5

# Figure 8b
python3 run_experiments.py lists [1,72,144,216,288] 100 0.75 5