
import os
import re
from tabulate import tabulate

param_list = ['ds','per','up','range','mfind','rs','n','p','z']
ds_list = ["arttree", "btree", "list_ro", "list", "dlist",
    "hash_block", "hash_block_lf"]

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
def readResultsFile(filename, throughputs, parameters):
  for line in open(filename, "r"):
    if not line.startswith('./'):
      continue
    param = toParam(line[2:])
    throughputs[toString(param)] = float(line.split(',')[-1])
    for p in param_list:
      if p not in parameters:
        parameters[p] = [param[p]]
      elif param[p] not in parameters[p]:
        parameters[p].append(param[p])

  for p in param_list:
    parameters[p].sort()
  
def print_table(throughput, parameters, row, col, params, rowvals=[], colvals=[]):
  p = params.copy()
  p[row] = '*'
  p[col] = '*'
  output = toString(p) + '\n========================================= \n\n'
  f = open(graphsfolder + '/' + toString(p) + ".txt", "w")
  if rowvals == []:
    rowvals = parameters[row]
  if colvals == []:
    colvals = parameters[col]
  headers = ['ds'] + colvals
  data = []
  for r in rowvals:
    row_data = [r]
    for c in colvals:
      p[row] = r
      p[col] = c
      # print(p)
      if toString(p) in throughputs:
        row_data.append(round(throughputs[toString(p)], 1))
      else:
        row_data.append('-')
    data.append(row_data)
  output += tabulate(data, headers=headers)
  print(output)
  print()
  f.write(output)
  f.close()

def print_table_mix_percent(throughput, parameters, mix_percent, params):
  params['up'] = mix_percent[0]
  params['mfind'] = mix_percent[1]
  params['range'] = mix_percent[2]
  params['rs'] = mix_percent[3]
  rowvals = parameters['ds']
  colvals = ['indirect', 'noshortcut', 'simple', 'per', 'ro', 'non_per']
  print_table(throughput, parameters, 'ds', 'per', params, rowvals, colvals)

def print_table_timestamp_inc(throughput, parameters, ds, per, size, mix_percents, params):
  p = params.copy()
  p['ds'] = ds
  p['n'] = size
  title = 'inc_policy_' + ds + '_' + per + '_size_' + str(size)
  output = title + '\n========================================= \n\n'
  f = open(graphsfolder + '/' + title + ".txt", "w")

  colvals = ['_rs', '_ws', '']
  headers = ['workload (up-mfind-range-rs)', 'read', 'write', 'switch']
  data = []
  for mix_percent in mix_percents:
    row_data = [mix_percent]
    for c in colvals:
      p['per'] = per + c
      p['up'] = mix_percent[0]
      p['mfind'] = mix_percent[1]
      p['range'] = mix_percent[2]
      p['rs'] = mix_percent[3]
      # print(p)
      if toString(p) in throughputs:
        row_data.append(round(throughputs[toString(p)], 1))
      else:
        row_data.append('-')
    data.append(row_data)
  output += tabulate(data, headers=headers)
  print(output)
  print()
  f.write(output)
  f.close()


filename = "aware.aladdin.cs.cmu.edu_10_21_22"
graphsfolder = 'graphs-' + filename
if not os.path.exists(graphsfolder):
  os.makedirs(graphsfolder)

throughputs = {}
parameters = {}

readResultsFile(filename, throughputs, parameters)
# print(parameters)

list_sizes = [10,1000]
tree_sizes = [1000,10000000]

small = {'list' : list_sizes[0], 'tree' : tree_sizes[0]}
large = {'list' : list_sizes[1], 'tree' : tree_sizes[1]}

mix_percents = [[5,0,0,0], [5,95,0,2], [5,95,0,16], [5,0,95,48], [5,5,0,16], [50,0,0,0], [50,50,0,16]]

params_small = {'n': small,
          'p': 143,
          'z': 0.99, }
params_large = {'n': large,
          'p': 143,
          'z': 0.99, }

for mix in mix_percents:
  print_table_mix_percent(throughputs, parameters, mix, params_small)
  print_table_mix_percent(throughputs, parameters, mix, params_large)

params = {'p': 143, 'z': 0.99,}

for ds in parameters['ds']:
  for per in ['per', 'simple']:
    sizes = []
    if 'list' in ds:
      sizes = list_sizes
    else:
      sizes = tree_sizes
    for n in sizes:
      print_table_timestamp_inc(throughputs, parameters, ds, per, n, mix_percents, params)


# rowvals = parameters['ds']
# colvals = ['indirect', 'noshortcut', 'simple', 'per', 'ro', 'non_per']
# print_table(throughputs, parameters, 'ds', 'per', params, rowvals, colvals)
