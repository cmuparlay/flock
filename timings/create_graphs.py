
import os
import re
from tabulate import tabulate
import matplotlib.pyplot as plt
import numpy as np

# input_files = ["ip-172-31-45-236_10_25_22", "ip-172-31-40-178_10_25_22"]
input_files = ["ip-172-31-35-105_12_28_22"]
output_folder = "dec28"
zipfs = [0, 0.99]

param_list = ['ds','per','up','range','mfind','rs','n','p','z']
ds_list = ["arttree", "btree", "dlist", "list", "hash_block_lf", "hash_block"]

num_tables = 0

combined_output = ""


# try to get LCFA running: https://github.com/kboyles8/lock-free-search-tree
# https://www.scienceopen.com/hosted-document?doi=10.14293/S2199-1006.1.SOR-.PPIV7TZ.v1
# compare with LFCA and MRLOCK


def custom_round(num):
  if num >= 100:
    return round(num)
  elif num >= 10:
    return round(num, 1)
  elif num >= 1:
    return round(num, 2)
  else:
    return round(num, 3)

def splitdsname(name):
  for ds in ds_list:
    if name == ds:
      return ds, 'non_per'
    if name.startswith(ds):
      return ds, name.replace(ds+'_','')

def toString(param):
  ds_name = param['ds']
  if param['per'] != 'non_per' and param['per']!='*':
    ds_name += '_' + param['per']
  if param['per'] == 'ro' and param['ds'] == 'list':
    return "invalid key"
  size = param['n']
  if type(param['n']) is dict and param['per'] != '*':
    if 'list' in param['ds']:
      size = param['n']['list']
    else:
      size = param['n']['tree']
  return ds_name + ',' + str(param['up']) + '%update,' + str(param['range']) + '%range,' + str(param['mfind']) + '%mfind,rs=' + str(param['rs']) + ',n=' + str(size) + ',p=' + str(param['p'])  + ',z=' + str(param['z'])

def toParam(string):
  numbers = re.sub('[^0-9.]', ' ', string).split()
  ds, per = splitdsname(string.split(',')[0])
  param = {'ds' : ds, 'per': per}
  p = param_list[2:]
  for i in range(len(p)):
    if '.' in numbers[i]:
      param[p[i]] = float(numbers[i])
    else:
      param[p[i]] = int(numbers[i])
  return param

# mutates throughputs and parameters
def readResultsFile(throughputs, parameters):
  throughputs_raw = {}
  for filename in input_files:
    for line in open(filename, "r"):
      if not line.startswith('./'):
        continue
      param = toParam(line[2:])
      key = toString(param)
      val = float(line.split(',')[-1])
      if key not in throughputs_raw:
        throughputs_raw[key] = [val]
      else:
        throughputs_raw[key].append(val)
      for p in param_list:
        if p not in parameters:
          parameters[p] = [param[p]]
        elif param[p] not in parameters[p]:
          parameters[p].append(param[p])
  for key in throughputs_raw:
    throughputs[key] = sum(throughputs_raw[key])/len(throughputs_raw[key])
  for p in param_list:
    parameters[p].sort()

def transpose(data_):
  data = []
  for x in data_[0]:
    data.append([])

  for r in range(len(data_)):
    for c in range(len(data_[0])):
      data[c].append(data_[r][c])
  return data


colors = ['C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6']

def plot_bar_graph(data_, headers, title, x_axis):
  x_labels = []
  for r in range(len(data_)):
    is_list = False
    for c in range(len(data_[0])):
      if c == 0 or data_[r][c] == '-':
        if c == 0:
          if data_[r][c] == 'list':
            x_labels.append('list (10x)')
            is_list = True
          else:
            x_labels.append(data_[r][c])
        data_[r][c] = 0
      elif is_list:
        # print("true")
        data_[r][c] = data_[r][c]*10
  data = np.transpose(np.array(data_, dtype=np.uint32))[1:]
  # data = [x if x != '-' else 0 for x in transpose(data_)[1:]]
  # print(data)
  barWidth = 1/(len(data)+3)
  fig = plt.subplots(figsize =(12, 8))
  # Set position of bar on X axis
  X = np.arange(len(data[0]))

  for i in range(len(data)):
    plt.bar(X + i*barWidth, data[i], color=colors[i], width = barWidth,
        edgecolor ='grey', label =headers[i+1])

  plt.xlabel('x_axis', fontweight ='bold', fontsize = 15)
  plt.ylabel('Throughput (Mop/s)', fontweight ='bold', fontsize = 15)
  plt.xticks([r + barWidth*(len(data)-1)/2 for r in range(len(data[0]))], x_labels)

  plt.title(title)

  plt.legend()
  plt.savefig(output_folder + '/' + title + ".png", bbox_inches='tight')
  plt.close('all')


def print_table(throughput, parameters, row, col, params, rowvals=[], colvals=[]):
  p = params.copy()
  p[row] = '*'
  p[col] = '*'
  title = toString(p)
  output = title + '\n========================================= \n\n'
  f = open(output_folder + '/' + title + ".txt", "w")
  if rowvals == []:
    rowvals = parameters[row]
  if 'hash_block' in rowvals:
    rowvals.remove('hash_block')
  if colvals == []:
    colvals = parameters[col]
  headers = ['ds'] + ['direct' if x == 'ro' else x for x in colvals]
  data = []
  for r in rowvals:
    row_data = [r]
    for c in colvals:
      p[row] = r
      p[col] = c
      # print(p)
      if toString(p) in throughputs and (r != 'hash_block_lf' or c != 'ver_lock_ls'):
        row_data.append(custom_round(throughputs[toString(p)]))
      else:
        row_data.append('-')
    data.append(row_data)
  output += tabulate(data, headers=headers) + '\n'
  global num_tables, combined_output
  num_tables += 1
  combined_output += output + '\n'
  print(output)
  print()
  f.write(output)
  f.close()
  plot_bar_graph(data, headers, title, "Data Structure")

def print_table_mix_percent(throughput, parameters, mix_percent, params):
  params['up'] = mix_percent[0]
  params['mfind'] = mix_percent[1]
  params['range'] = mix_percent[2]
  params['rs'] = mix_percent[3]
  rowvals = parameters['ds']
  colvals = ['indirect_ls', 'noshortcut_ls', 'ver_ls', 'ver_lock_ls', 'ver_ro_ls', 'non_per']
  print_table(throughput, parameters, 'ds', 'per', params, rowvals, colvals)

def print_table_timestamp_inc(throughput, parameters, ds, per, size, mix_percents, params):
  p = params.copy()
  p['ds'] = ds
  p['n'] = size
  title = 'inc_policy_' + ds + '_' + per + '_size_' + str(size) + '_z_' + str(params['z'])
  output = title + '\n========================================= \n\n'
  f = open(output_folder + '/' + title + ".txt", "w")

  colvals = ['_rs', '_ws', '_ls', '_hw', 'non_per']
  headers = ['workload (up-mfind-range-rs)', 'read', 'write', 'lazy', 'hardware', 'non_per']
  data = []
  for mix_percent in mix_percents:
    row_data = [mix_percent]
    for c in colvals:
      p['per'] = per + c
      if c == 'non_per': 
        p['per'] = c
      p['up'] = mix_percent[0]
      p['mfind'] = mix_percent[1]
      p['range'] = mix_percent[2]
      p['rs'] = mix_percent[3]
      # print(p)
      # print(toString(p))
      if toString(p) in throughputs:
        row_data.append(custom_round(throughputs[toString(p)]))
      else:
        row_data.append('-')
    data.append(row_data)
  output += tabulate(data, headers=headers) + '\n'
  global num_tables, combined_output
  num_tables += 1
  combined_output += output + '\n'
  print(output)
  f.write(output)
  f.close()
  plot_bar_graph(data, headers, title, "Workload")

def print_scalability_graphs(throughput, parameters, ds, per, size, mix_percents, params):
  
  return

def print_size_graphs(throughput, parameters, ds, per, size, mix_percents, params):

  return

if not os.path.exists(output_folder):
  os.makedirs(output_folder)

throughputs = {}
parameters = {}

readResultsFile(throughputs, parameters)

list_sizes = [100,1000]
tree_sizes = [100000,10000000]

small = {'list' : list_sizes[0], 'tree' : tree_sizes[0]}
large = {'list' : list_sizes[1], 'tree' : tree_sizes[1]}


# mix_percents = [[5,0,0,0], [5,25,0,4], [5,25,0,16], [5,0,95,48], [5,5,0,16], [50,0,0,0], [50,25,0,16]]
mix_percents = [[5,0,0,0], [50,0,0,0], [5,25,0,4], [5,25,0,16], [5,0,95,48]]

for zipf in zipfs:
  params_small = {'n': small,
            'p': parameters['p'][0],
            'z': zipf, }
  params_large = {'n': large,
            'p': parameters['p'][0],
            'z': zipf, }

  for mix in mix_percents:
    print_table_mix_percent(throughputs, parameters, mix, params_small)
    print_table_mix_percent(throughputs, parameters, mix, params_large)

  params = {'p': parameters['p'][0], 'z': zipf,}

  for ds in parameters['ds']:
    for per in ['ver']:
      sizes = []
      if 'list' in ds:
        sizes = list_sizes
      else:
        sizes = tree_sizes
      for n in sizes:
        print_table_timestamp_inc(throughputs, parameters, ds, per, n, mix_percents, params)

print(parameters)
print()

print('generated ' + str(num_tables) + ' tables')

f = open(output_folder + '/combined.txt', "w")
f.write(combined_output)
f.close()
# rowvals = parameters['ds']
# colvals = ['indirect', 'noshortcut', 'simple', 'per', 'ro', 'non_per']
# print_table(throughputs, parameters, 'ds', 'per', params, rowvals, colvals)
