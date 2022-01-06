import csv
import matplotlib as mpl
# mpl.use('Agg')
mpl.rcParams['grid.linestyle'] = ":"
mpl.rcParams.update({'font.size': 20})
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np
import os
import statistics as st

# paper_ver = False

class DSInfo:
    def __init__(self, color, marker, linestyle, name, binary, ds_type):
        self.binary = binary
        self.name = name
        self.color = color
        self.marker = marker
        self.linestyle = linestyle
        self.ds_type = ds_type

mk = ['o', 'v', '^', '1', 's', '+', 'x', 'D', '|', '>', '<',]

dsinfo = {"leaftree-trylock-lf"        :  DSInfo("C0", mk[0], "-", "leaftree-lf", "benchmark/leaftree -try_lock", "tree"),
          "arttree-trylock-lf"         :  DSInfo("C1", mk[1], "-", "arttree-lf",  "benchmark/arttree -try_lock", "tree"),
          "arttree_opt-trylock-lf"     :  DSInfo("C2", mk[2], "-", "arttree_opt-lf",  "benchmark/arttree_opt -try_lock", "tree"),
          "blockleaftree-trylock-lf"   :  DSInfo("C3", mk[3], "-", "blockleaftree-lf",  "benchmark/blockleaftree -try_lock", "tree"),
          "blockleaftree-b-trylock-lf" :  DSInfo("C4", mk[4], "-", "leaftreap-lf",  "benchmark/blockleaftree -b -try_lock", "tree"),
          "blockleaftree-lf"   :  DSInfo("C3", mk[3], "-", "blockleaftree-lf",  "benchmark/blockleaftree", "tree"),
          "blockleaftree-b-lf" :  DSInfo("C4", mk[4], "-", "leaftreap-lf",  "benchmark/blockleaftree -b", "tree"),
          "btree-trylock-lf"        :  DSInfo("C6", mk[6], "-", "btree-lf", "benchmark/btree -try_lock", "tree"),

          "leaftree-trylock-lb"        :  DSInfo("C0", mk[4], "--", "leaftree-bl", "benchmark/leaftree -no_help -try_lock", "tree"),
          "arttree-trylock-lb"         :  DSInfo("C1", mk[5], "--", "arttree-bl",  "benchmark/arttree -no_help -try_lock", "tree"),
          "arttree_opt-trylock-lb"     :  DSInfo("C2", mk[6], "--", "arttree_opt-bl",  "benchmark/arttree_opt -no_help -try_lock", "tree"),
          "blockleaftree-trylock-lb"   :  DSInfo("C3", mk[7], "--", "blockleaftree-bl",  "benchmark/blockleaftree -no_help -try_lock", "tree"),
          "blockleaftree-b-trylock-lb" :  DSInfo("C4", mk[8], "--", "leaftreap-bl",  "benchmark/blockleaftree -b -no_help -try_lock", "tree"),
          "blockleaftree-lb"   :  DSInfo("C3", mk[3], "--", "blockleaftree-bl",  "benchmark/blockleaftree -no_help", "tree"),
          "blockleaftree-b-lb" :  DSInfo("C4", mk[4], "--", "leaftreap-bl",  "benchmark/blockleaftree -b -no_help", "tree"),
          "btree-trylock-lb"        :  DSInfo("C6", mk[6], "--", "btree-bl", "benchmark/btree -no_help -try_lock", "tree"),

          "list-trylock-lf"            :  DSInfo("C0", mk[0], "-", "lazylist-lf",  "benchmark/list -try_lock", "list"),
          "dlist-trylock-lf"           :  DSInfo("C1", mk[1], "-", "dlist-lf",  "benchmark/dlist -try_lock", "list"),

          "list-trylock-lb"            :  DSInfo("C0", mk[2], "--", "lazylist-bl",  "benchmark/list -no_help -try_lock", "list"),
          "dlist-trylock-lb"           :  DSInfo("C1", mk[3], "--", "dlist-bl",  "benchmark/dlist -no_help -try_lock", "list"),

          "leaftree-lf"        :  DSInfo("C1", mk[1], "-", "leaftree-strictlock-lf", "benchmark/leaftree", "tree"),
          # "arttree-lf"         :  DSInfo("C1", mk[1], "-", "arttree-lf",  "arttree"),
          # "arttree_opt-lf"     :  DSInfo("C2", mk[2], "-", "arttree_opt-lf",  "arttree_opt"),
          # "blockleaftree-lf"   :  DSInfo("C3", mk[3], "-", "blockleaftree-lf",  "blockleaftree"),

          "leaftree-lb-nowait"        :  DSInfo("C2", mk[2], "--", "leaftree-strictlock-bl-nowait", "benchmark/leaftree -no_help", "tree"),
          "leaftree-lb"        :  DSInfo("C1", mk[3], "--", "leaftree-strictlock-bl", "benchmark/leaftree -no_help -wait", "tree"),
          # "arttree-lb"         :  DSInfo("C1", mk[5], "--", "arttree-lb",  "arttree -no_help"),
          # "arttree_opt-lb"     :  DSInfo("C2", mk[6], "--", "arttree_opt-lb",  "arttree_opt -no_help"),
          # "blockleaftree-lb"   :  DSInfo("C3", mk[7], "--", "blockleaftree-lb",  "blockleaftree -no_help"),

          # "list-lf"            :  DSInfo("C0", mk[0], "-", "list-lf",  "list"),
          # "dlist-lf"           :  DSInfo("C1", mk[1], "-", "dlist-lf",  "dlist"),

          # "list-lb"            :  DSInfo("C0", mk[2], "--", "list-lb",  "list -no_help"),
          # "dlist-lb"           :  DSInfo("C1", mk[3], "--", "dlist-lb",  "dlist -no_help"),

          # "hash-lf"            :  DSInfo("C0", mk[0], "-", "hash-lf",  "hash", "tree"),
          "hash_optimistic-lf" :  DSInfo("C5", mk[9], "-", "hashtable-strictlock-lf",  "benchmark/hash_optimistic", "tree"),
          "hash_optimistic-trylock-lf" :  DSInfo("C5", mk[9], "-", "hashtable-lf",  "benchmark/hash_optimistic -try_lock", "tree"),

          # "hash-lb"            :  DSInfo("C0", mk[2], "--", "hash-lb",  "hash -no_help", "tree"),
          "hash_optimistic-lb" :  DSInfo("C5", mk[10], "--", "hashtable-strictlock-bl",  "benchmark/hash_optimistic -no_help", "tree"),
          "hash_optimistic-trylock-lb" :  DSInfo("C5", mk[10], "--", "hashtable-bl",  "benchmark/hash_optimistic -no_help -try_lock", "tree"),

          "bronson"         :  DSInfo("C4", mk[4], "--", "bronson",  "setbench/bin/bronson_bst_bench", "tree"),
          "drachsler"       :  DSInfo("C5", mk[5], "--", "drachsler",  "setbench/bin/drachsler_bst_bench", "tree"),
          "ellen"           :  DSInfo("C6", mk[6], "-", "ellen",  "setbench/bin/ellen_bst_bench", "tree"),
          "guerraoui"       :  DSInfo("C7", mk[7], "--", "guerraoui",  "setbench/bin/guerraoui_bst_bench", "tree"),
          "natarajan"       :  DSInfo("C8", mk[8], "-", "natarajan",  "setbench/bin/natarajan_bst_bench", "tree"),
          "chromatic"       :  DSInfo("C9", mk[9], "-", "chromatic",  "setbench/bin/chromatic_bst_bench", "tree"),
          "abtree"          :  DSInfo("C9", mk[9], "-", "abtree",  "setbench/bin/brown_abtree_bench", "tree"),
          "sri_abtree"      :  DSInfo("C9", mk[9], "-", "sri_abtree",  "setbench/bin/srivastava_abtree_bench", "tree"),
          "sri_abtree_mcs"  :  DSInfo("C8", mk[8], "--", "sri_abtree_mcs",  "setbench/bin/srivastava_abtree_mcs_bench", "tree"),
          "sri_abtree_pub"  :  DSInfo("C7", mk[7], "--", "sri_abtree_pub",  "setbench/bin/srivastava_abtree_pub_bench", "tree"),
          "scx_bst"         :  DSInfo("C9", mk[9], "--", "scx_bst",  "scx_bst", "tree"),

          "harris_list"     :  DSInfo("C2", mk[4], "-", "harris_list",  "benchmark/harris_list", "list"),
          "harris_list_opt"     :  DSInfo("C3", mk[5], "-", "harris_list_opt",  "benchmark/harris_list_opt", "list"),
          "mwcas_dlist"     :  DSInfo("C4", mk[6], "-", "mwcas_dlist",  "pmwcas/build/doubly_linked_list_test_sets", "list"),
}

# datastructures = ["leaftree", "arttree"]
ds_list = { 'tree-ours-trylock-lb'        : ['leaftree-trylock-lb', 'arttree-trylock-lb', 'arttree_opt-trylock-lb', 'blockleaftree-trylock-lb', 'blockleaftree-b-trylock-lb'],
            'tree-ours-trylock-lf'        : ['leaftree-trylock-lf', 'arttree-trylock-lf', 'arttree_opt-trylock-lf', 'blockleaftree-trylock-lf', 'blockleaftree-b-trylock-lf'],
            'tree-theirs'                 : ['bronson', 'drachsler', 'natarajan'], #, 'chromatic', 'ellen', 'guerraoui', ],

            'list-ours-trylock-lb'        : ['list-trylock-lb', 'dlist-trylock-lb'], 
            'list-ours-trylock-lf'        : ['list-trylock-lf', 'dlist-trylock-lf'], 
            'list-theirs'                 : ['harris_list'], #, 'mwcas_dlist', harris_list_opt'],

            'trees'           : ['chromatic', 'leaftree-trylock-lb', 'leaftree-trylock-lf', 'bronson', 'drachsler', 'natarajan', 'ellen', 'scx_bst'],
            'rtrees'          : ['leaftree-trylock-lb', 'leaftree-trylock-lf', 'arttree-trylock-lb', 'blockleaftree-b-lb', 'blockleaftree-b-trylock-lb', 'arttree-trylock-lf', 'blockleaftree-b-lf', 'blockleaftree-b-trylock-lf', 'hash_optimistic-trylock-lf', 'hash_optimistic-trylock-lb', ], # 'arttree_opt-trylock-lb', 'arttree_opt-trylock-lf', 
            'lists'           : ['list-trylock-lb', 'dlist-trylock-lb', 'list-trylock-lf', 'dlist-trylock-lf', 'harris_list', 'harris_list_opt'],
            'our-lists'           : ['list-trylock-lb', 'list-trylock-lf'],

            # 'leftovers-list'       : ['list-trylock-lb', 'dlist-trylock-lb', 'list-trylock-lf', 'dlist-trylock-lf', 'harris_list', 'harris_list_opt'],
            'leftovers-list'       : ['harris_list', 'harris_list_opt'],
            'leftovers-tree'       : ['leaftree-trylock-lb', 'leaftree-trylock-lf', 'leaftree-lb', 'leaftree-lf'],
            # 'leftovers-rtree'      : ['hash_optimistic-trylock-lf', 'hash_optimistic-trylock-lb'],
            'leftovers-rtree'      : ['blockleaftree-b-lb', 'blockleaftree-b-lf'],

            'test_list'       : ['list-trylock-lf'],
            'test_tree'       : ['bronson'],
            'test_chromatic'       : ['chromatic'],
            'test_scx_bst'       : ['scx_bst'],
            # 'test_abtree'       : ['abtree','sri_abtree','sri_abtree_mcs','sri_abtree_pub','chromatic'],
            'test_abtree'       : ['chromatic', 'sri_abtree_mcs','sri_abtree_pub'],
            # 'test_abtree'       : ['abtree'],

            'test_mwcas_dlist'       : ['mwcas_dlist'],
            'test_arttree'       : ['arttree-trylock-lb', "arttree-trylock-lf"],

            'try_lock_exp'   : ['leaftree-lb', 'leaftree-lf', 'leaftree-trylock-lf', 'leaftree-trylock-lb', ],
}

# datastructures = {}
# datastructures['lists'] = ds_list['list-ours-trylock-lb'] + ds_list['list-ours-trylock-lf'] + ds_list['list-theirs']
# datastructures['tree'] = 

def toString(algname, th, ratio, maxkey, alpha):
  if alpha == -1:
    return algname + '-' + str(th) + 't-' + str(maxkey) + 'k-' + str(ratio) + 'up-uniform'
  else:
    return algname + '-' + str(th) + 't-' + str(maxkey) + 'k-' + str(ratio) + 'up-zipf-' + str(alpha)

# def export_legend(legend, filename="legend.pdf", expand=[-5,-5,5,5]):
#     fig  = legend.figure
#     fig.canvas.draw()
#     bbox  = legend.get_window_extent()
#     bbox = bbox.from_extents(*(bbox.extents + np.array(expand)))
#     bbox = bbox.transformed(fig.dpi_scale_trans.inverted())
#     fig.savefig(filename, dpi="figure", bbox_inches=bbox)

def export_legend(legend, filename="legend.pdf"):
    fig  = legend.figure
    fig.canvas.draw()
    bbox  = legend.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(filename, dpi="figure", bbox_inches=bbox)

def avg(numlist):
  # if len(numlist) == 1:
  #   return numlist[0]
  total = 0.0
  length = 0
  for num in numlist:
    length=length+1
    total += float(num)
  if length > 0:
    return 1.0*total/length
  else:
    return -1;

def readResultsFile(filename, throughput, stddev, threads, ratios, maxkeys, alphas, algs):
  throughputRaw = {}
  alg = ""
  th = ""
  ratio = ""
  maxkey = ""
  alpha = ""
  warm_up_runs = 0

  # read csv into throughputRaw
  file = open(filename, 'r')
  for line in file.readlines():
    line = line.strip();
    if line.find('warm_up_runs') != -1:
      warm_up_runs = int(line.split(' ')[1])
    elif line.find('datastructure:') != -1:
      alg = line.split(' ')[1]
      alpha = 0 # indicates uniform distribution
    elif line.find('running on ') != -1:
      th = int(line.split(' ')[2])
      maxkey = int(line.split(' ')[-1])
    elif line.find('zipf') != -1:
      if line.find('parameter') != -1:
        alpha = float(line.split(' ')[-1])
    elif line.find(' updates') != -1:
      ratio = int(line.split('%')[0])
    elif len(line.split(',')) == 3 and line.find('update') != -1:
      if alg not in algs:
        algs.append(alg)
      if th not in threads:
        threads.append(th)
      if ratio not in ratios:
        ratios.append(ratio)
      if maxkey not in maxkeys:
        maxkeys.append(maxkey)
      if alpha not in alphas:
        alphas.append(alpha)
      key = toString(alg, th, ratio, maxkey, alpha)
      if key not in throughputRaw:
        throughputRaw[key] = []
      throughputRaw[key].append(float(line.split(',')[-1]))

  # print(throughputRaw)
  # Average througputRaw into throughput

  for key in throughputRaw:
    results = throughputRaw[key][warm_up_runs:]
    # print(results)
    throughput[key] = avg(results)
    stddev[key] = st.pstdev(results)
    # print(avgResult)

  # print(throughput)

def plot_alpha_graph(throughput, stddev, thread, ratio, maxkey, alphas, algs, graph_name, paper_ver=False):
  # print(graphtitle)
  graphtitle = graph_name + '-' + str(thread) + 'th-' + str(maxkey) + 'size-' + str(ratio) + 'up'
  if paper_ver:
    outputFile = 'graphs/' + graphtitle.replace('.', '') + '.pdf'
    mpl.rcParams.update({'font.size': 25})
  else:
    outputFile = 'graphs/' + graphtitle + '.png'

  ymax = 0
  series = {}
  error = {}
  for alg in algs:
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and (bench.find('-0rq') == -1 or bench.find('2000000000') != -1):
    #   continue
    # if toString3(alg, 1, bench) not in results:
    #   continue
    series[alg] = []
    error[alg] = []
    for alpha in alphas:
      key = toString(alg, thread, ratio, maxkey, alpha)
      if key not in throughput:
        del series[alg]
        del error[alg]
        break
      series[alg].append(throughput[key])
      error[alg].append(stddev[key])
  
  if len(series) < 3:
    return

  fig, axs = plt.subplots()
  # fig = plt.figure()
  opacity = 0.8
  rects = {}
  
  xpos = np.arange(start=0, stop=len(alphas))

  for alg in algs:
    alginfo = dsinfo[alg]
    if alg not in series:
      continue
    ymax = max(ymax, max(series[alg]))
    # rects[alg] = axs.errorbar(xpos, series[alg], yerr=error[alg],
    # if graph_name == 'try_vs_strict_lock' and alginfo.name.find('leaftree-strict') == -1:
    if 'leaftree-lf' in algs and alginfo.name.find('leaftree-strict') == -1:
      rects[alg] = axs.plot(xpos, series[alg],
      alpha=opacity,
      color=alginfo.color,
      linewidth=3.0,
      #hatch=hatch[ds],
      linestyle=alginfo.linestyle,
      marker=alginfo.marker,
      markersize=14,
      label=alginfo.name.replace('leaftree-', 'leaftree-trylock-'))
    else:
      rects[alg] = axs.plot(xpos, series[alg],
        alpha=opacity,
        color=alginfo.color,
        linewidth=3.0,
        #hatch=hatch[ds],
        linestyle=alginfo.linestyle,
        marker=alginfo.marker,
        markersize=14,
        label=alginfo.name)

  plt.xticks(xpos, alphas)
  axs.set_ylim(bottom=-0.02*ymax)
  # plt.xticks(threads, threads)
  # axs.set_xlabel('Number of threads')
  # axs.set_ylabel('Throughput (Mop/s)')
  axs.set(xlabel='Zipfian parameter', ylabel='Throughput (Mop/s)')
  legend_x = 1
  legend_y = 0.5 
  # if this_file_name == 'Update_heavy_with_RQ_-_100K_Keys':
  #   plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))

  # plt.legend(framealpha=0.0)

  plt.grid()
  if 'leaftree-lf' in algs:
    plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  elif not paper_ver:
    plt.title(graphtitle)
    plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  plt.savefig(outputFile, bbox_inches='tight')
  plt.close('all')

def plot_alpha_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name, paper_ver=False):
  for thread in threads:
    for size in maxkeys:
      for ratio in ratios:
        sufficient_datapoints = False
        for alg in algs:
          num_datapoints = 0
          for alpha in alphas:
            if toString(alg, thread, ratio, size, alpha) in throughput:
              num_datapoints += 1
          if num_datapoints > 2:
            sufficient_datapoints = True
        if sufficient_datapoints:
          plot_alpha_graph(throughput, stddev, thread, ratio, size, alphas, algs, graph_name, paper_ver)

def plot_size_graph(throughput, stddev, thread, ratio, maxkeys, alpha, algs, graph_name, paper_ver=False):
  # print(graphtitle)
  graphtitle = graph_name + '-' + str(thread) + 'th-' + str(ratio) + 'up-' + str(alpha) + 'alpha'
  if paper_ver:
    outputFile = 'graphs/' + graphtitle.replace('.', '') + '.pdf'
    mpl.rcParams.update({'font.size': 25})
  else:
    outputFile = 'graphs/' + graphtitle + '.png'

  ymax = 0
  series = {}
  error = {}
  for alg in algs:
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and (bench.find('-0rq') == -1 or bench.find('2000000000') != -1):
    #   continue
    # if toString3(alg, 1, bench) not in results:
    #   continue
    series[alg] = []
    error[alg] = []
    for maxkey in maxkeys:
      key = toString(alg, thread, ratio, maxkey, alpha)
      if key not in throughput:
        del series[alg]
        del error[alg]
        break
      series[alg].append(throughput[key])
      error[alg].append(stddev[key])
  if len(series) < 3:
    return

  fig, axs = plt.subplots()
  # fig = plt.figure()
  opacity = 0.8
  rects = {}
  
  for alg in algs:
    alginfo = dsinfo[alg]
    if alg not in series:
      continue
    ymax = max(ymax, max(series[alg]))
    # rects[alg] = axs.errorbar(maxkeys, series[alg], yerr=error[alg],
    rects[alg] = axs.plot(maxkeys, series[alg],
      alpha=opacity,
      color=alginfo.color,
      #hatch=hatch[ds],
      linewidth=3.0,
      linestyle=alginfo.linestyle,
      marker=alginfo.marker,
      markersize=14,
      label=alginfo.name)

  # if maxkeys[-1] > 1000000:
  #   plt.axvline(1000000, linestyle='--', color='grey') 
  axs.set_xscale('log')
  axs.set_ylim(bottom=-0.02*ymax)
  # plt.xticks(threads, threads)
  # axs.set_xlabel('Number of threads')
  # axs.set_ylabel('Throughput (Mop/s)')
  axs.set(xlabel='Datastructure size', ylabel='Throughput (Mop/s)')
  legend_x = 1
  legend_y = 0.5 
  # if this_file_name == 'Update_heavy_with_RQ_-_100K_Keys':
  #   plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))

  # plt.legend(framealpha=0.0)
  plt.grid()
  if not paper_ver:
    plt.title(graphtitle)
    plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  plt.savefig(outputFile, bbox_inches='tight')
  plt.close('all')

def plot_size_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name, paper_ver=False):
  for thread in threads:
    for ratio in ratios:
      for alpha in alphas:
        sufficient_datapoints = False
        xaxis = []
        for alg in algs:
          num_datapoints = 0
          for size in maxkeys:
            if toString(alg, thread, ratio, size, alpha) in throughput:
              num_datapoints += 1
              if size not in xaxis:
                xaxis.append(size)
          if num_datapoints > 2:
            sufficient_datapoints = True
        if sufficient_datapoints:
          plot_size_graph(throughput, stddev, thread, ratio, xaxis, alpha, algs, graph_name, paper_ver)


def plot_ratio_graph(throughput, stddev, thread, ratios, maxkey, alpha, algs, graph_name, paper_ver=False):
  # print(graphtitle)
  graphtitle = graph_name + '-' + str(thread) + 'th-' + str(maxkey) + 'size-' + str(alpha) + 'alpha'
  if paper_ver:
    outputFile = 'graphs/' + graphtitle.replace('.', '') + '.pdf'
    mpl.rcParams.update({'font.size': 25})
  else:
    outputFile = 'graphs/' + graphtitle + '.png'

  ymax = 0
  series = {}
  error = {}
  for alg in algs:
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and (bench.find('-0rq') == -1 or bench.find('2000000000') != -1):
    #   continue
    # if toString3(alg, 1, bench) not in results:
    #   continue
    series[alg] = []
    error[alg] = []
    for ratio in ratios:
      key = toString(alg, thread, ratio, maxkey, alpha)
      if key not in throughput:
        del series[alg]
        del error[alg]
        break
      series[alg].append(throughput[key])
      error[alg].append(stddev[key])
  if len(series) < 3:
    return

  fig, axs = plt.subplots()
  # fig = plt.figure()
  opacity = 0.8
  rects = {}
  
  xpos = np.arange(start=0, stop=len(ratios))

  for alg in algs:
    alginfo = dsinfo[alg]
    if alg not in series:
      continue
    ymax = max(ymax, max(series[alg]))
    # rects[alg] = axs.errorbar(xpos, series[alg], yerr=error[alg],
    rects[alg] = axs.plot(xpos, series[alg],
      alpha=opacity,
      color=alginfo.color,
      linewidth=3.0,
      #hatch=hatch[ds],
      linestyle=alginfo.linestyle,
      marker=alginfo.marker,
      markersize=14,
      label=alginfo.name)

  plt.xticks(xpos, ratios)
  axs.set_ylim(bottom=-0.02*ymax)
  # plt.xticks(threads, threads)
  # axs.set_xlabel('Number of threads')
  # axs.set_ylabel('Throughput (Mop/s)')
  axs.set(xlabel='Update percentage', ylabel='Throughput (Mop/s)')
  legend_x = 1
  legend_y = 0.5 
  # if this_file_name == 'Update_heavy_with_RQ_-_100K_Keys':
  #   plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))

  # plt.legend(framealpha=0.0)
  plt.grid()
  if not paper_ver:
    plt.title(graphtitle)
    plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  plt.savefig(outputFile, bbox_inches='tight')
  plt.close('all')

def plot_ratio_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name, paper_ver=False):
  for thread in threads:
    for size in maxkeys:
      for alpha in alphas:
        sufficient_datapoints = False
        for alg in algs:
          num_datapoints = 0
          for ratio in ratios:
            if toString(alg, thread, ratio, size, alpha) in throughput:
              num_datapoints += 1
          if num_datapoints > 2:
            sufficient_datapoints = True
        if sufficient_datapoints:
          plot_ratio_graph(throughput, stddev, thread, ratios, size, alpha, algs, graph_name, paper_ver)


def plot_scalability_graph(throughput, stddev, threads, ratio, maxkey, alpha, algs, graph_name, paper_ver=False):
  # print(graphtitle)
  graphtitle = graph_name + '-' + str(ratio) + 'up-' + str(maxkey) + 'size-' + str(alpha) + 'alpha'
  if paper_ver:
    outputFile = 'graphs/' + graphtitle.replace('.', '') + '.pdf'
    mpl.rcParams.update({'font.size': 25})
  else:
    outputFile = 'graphs/' + graphtitle + '.png'
  print("ploting " + outputFile)

  ymax = 0
  series = {}
  error = {}
  for alg in algs:
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and (bench.find('-0rq') == -1 or bench.find('2000000000') != -1):
    #   continue
    # if toString3(alg, 1, bench) not in results:
    #   continue
    series[alg] = []
    error[alg] = []
    for th in threads:
      key = toString(alg, th, ratio, maxkey, alpha)
      if key not in throughput:
        del series[alg]
        del error[alg]
        break
      series[alg].append(throughput[key])
      error[alg].append(stddev[key])
  # if len(series) < 3:
  #   return
  # print("ploting " + outputFile)
  fig, axs = plt.subplots()
  # fig = plt.figure()
  opacity = 0.8
  rects = {}
  
  for alg in algs:
    alginfo = dsinfo[alg]
    if alg not in series:
      continue
    ymax = max(ymax, max(series[alg]))
    # rects[alg] = axs.errorbar(threads, series[alg], yerr=error[alg],
    rects[alg] = axs.plot(threads, series[alg],
      alpha=opacity,
      color=alginfo.color,
      linewidth=3.0,
      #hatch=hatch[ds],
      linestyle=alginfo.linestyle,
      marker=alginfo.marker,
      markersize=14,
      label=alginfo.name)

  axs.set_ylim(bottom=-0.02*ymax)
  # plt.xticks(threads, threads)
  # axs.set_xlabel('Number of threads')
  # axs.set_ylabel('Throughput (Mop/s)')
  plt.axvline(144, linestyle='--', color='grey') 
  axs.set(xlabel='Number of threads', ylabel='Throughput (Mop/s)')
  legend_x = 1
  legend_y = 0.5 
  # if this_file_name == 'Update_heavy_with_RQ_-_100K_Keys':
  #   plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))



  # plt.legend(framealpha=0.0)
  plt.grid()

  if not paper_ver:
    plt.title(graphtitle)
    plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  
  plt.savefig(outputFile, bbox_inches='tight')

  if paper_ver:
    if graph_name == 'lists':
      legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=2, framealpha=0.0)
    elif graph_name == 'sets':
      legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=3, framealpha=0.0)
    else:
      legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=8, framealpha=0.0)
    # outputFile = 'graphs/' + graph_name + '_legend.pdf'
    # legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=7, framealpha=0.0)
    export_legend(legend, 'graphs/' + graph_name + '_legend.pdf')
    # plt.close('all')
    # return
  
  plt.close('all')

def plot_scalability_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name, paper_ver=False):
  for ratio in ratios:
    for size in maxkeys:
      for alpha in alphas:
        sufficient_datapoints = False
        for alg in algs:
          num_datapoints = 0
          for th in threads:
            if toString(alg, th, ratio, size, alpha) in throughput:
              num_datapoints += 1
          # print(num_datapoints)
          if num_datapoints > 2:
            sufficient_datapoints = True
        # print(sufficient_datapoints)
        if sufficient_datapoints:
          plot_scalability_graph(throughput, stddev, threads, ratio, size, alpha, algs, graph_name, paper_ver)


def plot_all_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name):
  print("ploting scalability graphs for: " + graph_name)
  plot_scalability_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name)
  print("ploting ratio graphs for: " + graph_name)
  plot_ratio_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name)
  print("ploting alpha graphs for: " + graph_name)
  plot_alpha_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name)
  print("ploting size graphs for: " + graph_name)
  plot_size_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, algs, graph_name)

if __name__ == "__main__":
  throughput = {}
  stddev = {}
  threads = []
  ratios = []
  maxkeys = []
  alphas = []
  algs = []

  for filename in input_files:
    readResultsFile(filename, throughput, stddev, threads, ratios, maxkeys, alphas, algs)

  threads.sort()
  ratios.sort()
  maxkeys.sort()
  alphas.sort()

  print('threads: ' + str(threads))
  print('update ratios: ' + str(ratios))
  print('maxkeys: ' + str(maxkeys))
  print('alphas: ' + str(alphas))
  print('algs: ' + str(algs))

  plot_scalability_graph(throughput, stddev, threads, 50, 100000, 0.75, ds_list["try_lock_exp"], "try_vs_strict_lock", True)
  plot_scalability_graph(throughput, stddev, threads, 50, 100000, 0.75, ds_list["trees"], "trees", True)
  plot_scalability_graph(throughput, stddev, threads, 50, 100, 0.75, ds_list["lists"], "lists", True)
  plot_scalability_graph(throughput, stddev, threads, 50, 100000, 0.75, ds_list["rtrees"], "rtrees", True)

  plot_all_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, ds_list["try_lock_exp"], "try_vs_strict_lock")
  plot_all_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, ds_list["trees"], "trees")
  plot_all_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, ds_list["lists"], "lists")
  plot_all_graphs(throughput, stddev, threads, ratios, maxkeys, alphas, ds_list["rtrees"], "rtrees")

