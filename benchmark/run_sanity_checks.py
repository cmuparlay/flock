
import os

datastructures = ["arttree", "btree", "list", "list_ro", "dlist",
    "hash_block", "hash_block_lf"]
datastructures_ro = ['btree', 'list_ro']

persistance = ['_noshortcut', '_indirect', '_per', '_per_lock', 
               '_per_rs', '_per_ws']

for ds in datastructures_ro:
  cmd = './' + ds + '_ro' + ' -i'
  os.system('echo \'' + cmd + '\'')
  os.system(cmd)

for ds in datastructures:
  for per in persistance:
    cmd = './' + ds + per + ' -i'
    os.system('echo \'' + cmd + '\'')
    os.system(cmd)

for ds in datastructures_ro:
  cmd = './' + ds + '_ro'
  if 'list' in ds:
    cmd += ' -n 100'
  os.system('echo \'' + cmd + '\'')
  os.system(cmd)

for ds in datastructures:
  for per in persistance:
    cmd = './' + ds + per 
    if 'list' in ds:
      cmd += ' -n 100'
    os.system('echo \'' + cmd + '\'')
    os.system(cmd)
