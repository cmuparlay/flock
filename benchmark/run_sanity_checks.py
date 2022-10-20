
import system

datastructures = ["arttree", "btree", "list", "list_ro", "dlist",
    "hash_block", "hash_block_lf"]
datastructures_ro = ['btree', 'list_ro']

persistance = ['', '_noshortcut', '_indirect', '_per', '_per_lock', 
               '_read_stamp', '_write_stamp']

for ds in datastructures_ro:
  system.os(ds + '_ro')

for ds in datastructures:
  for per in persistance:
    system.os(ds + per)
