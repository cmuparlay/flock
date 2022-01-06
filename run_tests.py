
import os
from create_graphs import *

ds_list = {"lists": ['list-trylock-lb', 'dlist-trylock-lb', 'list-trylock-lf', 'dlist-trylock-lf', 'harris_list', 'harris_list_opt'],
           "trees": ['leaftree-trylock-lb', 'leaftree-trylock-lf', 'bronson', 'drachsler', 'natarajan', 'ellen'],
           "sets": ['leaftree-trylock-lb', 'leaftree-trylock-lf', 'arttree-trylock-lb', 'blockleaftree-b-trylock-lb', 'arttree-trylock-lf', 'blockleaftree-b-trylock-lf', 'hash_optimistic-trylock-lf', 'hash_optimistic-trylock-lb'],
           "with-vs-try-lock": ['leaftree-trylock-lb', 'leaftree-trylock-lf', 'leaftree-lf', 'leaftree-lb']}

for _, datastructures in ds_list.items():
  for ds in datastructures:
    os.system("echo \"testing: " + ds + "\"")
    os.system(dsinfo[ds].binary + " -i")