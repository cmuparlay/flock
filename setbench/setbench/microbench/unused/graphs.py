import sqlite3
import os.path
from os.path import isdir
from os import mkdir
import numpy as np
import matplotlib as mpl
mpl.use('Agg')

import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import sys
import math

clean = 0
if "-clean" in sys.argv:
    clean = 1

if clean: 
    print "Generating clean graphs using Matplotlib version " + mpl.__version__
    font = {'weight' : 'normal',
            'size'   : 26}

    mpl.rc('font', **font)
else: print "Generating graphs using Matplotlib version " + mpl.__version__
    
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

DB_FILENAME = "results.db"

if not os.path.isfile(DB_FILENAME):
    print "Database file '" + DB_FILENAME + "' does not exist!"
    print "Run python create_db.py first."
    exit()

class StdevFunc:
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 1

    def step(self, value):
        if value is None:
            return
        tM = self.M
        self.M += (value - tM) / self.k
        self.S += (value - tM) * (value - self.M)
        self.k += 1

    def finalize(self):
        if self.k < 3:
            return None
        return math.sqrt(self.S / (self.k-2))

metric = "(throughput/1000000.0)"
metric_name = "throughput"
metric1 =  "(update_throughput/1000000.0)"
metric1_name = "updates"
metric2 = "(find_throughput/1000000.0)"
metric2_name = "finds"
metric3 = "(rq_throughput/1000000.0)"
metric3_name = "rqs"
  

datastructures= ["abtree" ,"bst" ,"lflist","lazylist","citrus","skiplistlock"]
logdatastructures = ["abtree","bst","citrus","skiplistlock"]
rludatastructures = ["citrus","lazylist"]

def is_valid_key_range(ds,key_range):
    if key_range == "10000000" and ds!= "abtree": return False
    if key_range == "1000000" and ds!= "abtree": return False
    if key_range == "100000" and ds not in logdatastructures: return False
    if key_range == "10000" and ds in logdatastructures: return False
    if key_range == "1000" and ds in logdatastructures: return False
    if clean and key_range == "100000" and ds== "abtree": return False
    return True

dsToAlgs = { "abtree" :         ["rwlock", "htm_rwlock", "lockfree"],
             "bst" :            ["rwlock", "htm_rwlock", "lockfree"],
             "lflist" :         ["rwlock", "htm_rwlock", "lockfree", "snapcollector"],
             "lazylist" :       ["rwlock", "htm_rwlock", "lockfree", "rlu"],
             "citrus" :         ["rwlock", "htm_rwlock", "lockfree", "rlu"],
             "skiplistlock" :   ["rwlock", "htm_rwlock", "lockfree", "snapcollector"]
            }
            
dsToName = { "abtree" :         "ABTree",
             "bst" :            "Lock-free BST",
             "lflist" :         "Lock-free List",
             "lazylist" :       "LazyList",
             "citrus" :         "Citrus",
             "skiplistlock" :   "SkipList"
            }
            
dsToHatch = { "abtree" :         [ "", '.', '/', 'O', '-', '*' ],
                "bst" :          [ "", '.', '/', 'O', '-', '*' ],
                "lflist" :       [ "", '.', '/', '-', 'O', '-', '*' ],
                "lazylist" :     [ "", '.', '/', 'x', 'O', '-', '*' ],
                "citrus" :       [ "", '.', '/', 'x', 'O', '-', '*' ],
                "skiplistlock" : [ "", '.', '/', '-', 'O', '-', '*' ]
            }

dsToColor = {"abtree" :       ['C0','C1','C2','C5'],
             "bst" :          ['C0','C1','C2','C5'],
             "lflist" :       ['C0','C1','C2','C3','C5'],
             "lazylist" :     ['C0','C1','C2','C4','C5'],
             "citrus" :       ['C0','C1','C2','C4','C5'],
             "skiplistlock" : ['C0','C1','C2','C3','C5']
            }
           
dsToMarker = {"abtree" :        ["o", "^", "s", "x"],
             "bst" :            ["o", "^", "s", "x"],
             "lflist" :         ["o", "^", "s", "P", "x"],
             "lazylist" :       ["o", "^", "s", "X", "x"],
             "citrus" :         ["o", "^", "s", "X", "x"],
             "skiplistlock" :   ["o", "^", "s", "P", "x"]
            }

markerToSize = {    "o":22, #lock
                    "^":24, #HTM
                    "s":22, #lockfree
                    "+":26, 
                    "D":18, 
                    "x":24, #unsafe
                    "X":22, #RLU
                    "P":22  #snapcollector
                }

dsToStyle = {"abtree" :         ['-', '-', '-', ':'],
             "bst" :            ['-', '-', '-', ':'],
             "lflist" :         ['-', '-', '-', '--', ':'],
             "lazylist" :       ['-', '-', '-', '--', ':'],
             "citrus" :         ['-', '-', '-', '--', ':'],
             "skiplistlock" :   ['-', '-', '-', '--', ':']
            }
            
line_width = 5
conn = sqlite3.connect(DB_FILENAME)
#cursor = conn.cursor()

conn.create_aggregate("STDEV", 1, StdevFunc)

if not isdir("graphs"):
    mkdir("graphs")
if not isdir("graphs/unsafe"):
    mkdir("graphs/unsafe")
outdir = "graphs/"

allalgs =[ "rwlock", "htm_rwlock", "lockfree", "snapcollector", "rlu", "unsafe"]
hatches = [ None, '.', '/', 'x', '+', 'O', '-', '*' ] #default for plot_clustered_stacked() 
hatches_mult = 2

#based on https://stackoverflow.com/questions/22787209/how-to-have-clusters-of-stacked-bars-with-python-pandas
#fixed the positionning of the bars so that the last bar is not cutoff
#resized the width to have spaces between the bars 
def plot_clustered_stacked(dfall, labels=None, H=hatches , title="multiple stacked bar plot", **kwargs):
    """Given a list of dataframes, with identical columns and index, create a clustered stacked bar plot. 
labels is a list of the names of the dataframe, used for the legend
title is a string for the title of the plot
H is the hatch used for identification of the different dataframe"""

    n_df = len(dfall)
    n_col = len(dfall[0].columns) 
    n_ind = len(dfall[0].index)
    axe = plt.subplot(111)

    for df in dfall : # for each data frame
        axe = df.plot(kind="bar",
                      linewidth=0,
                      stacked=True,
                      ax=axe,
                      legend=False,
                      grid=True,
                      **kwargs)  # make bar plots

    h,l = axe.get_legend_handles_labels() # get the handles we want to modify
    for i in range(0, n_df * n_col, n_col): # len(h) = n_col * n_df
        for j, pa in enumerate(h[i:i+n_col]):
            for rect in pa.patches: # for each index
                rect.set_x(rect.get_x() + 1 / float(n_df + 2) * i / float(n_col))
                #a if condition else b
                chatch = H[int(i / (n_col))]*hatches_mult if H[int(i / (n_col))] else None
                rect.set_hatch(chatch) #edited part     
                rect.set_width((1 / float(n_df + 2))-0.02)

    axe.set_xticks((np.arange(0, 2 * n_ind, 2) + 1 / float(n_df + 2)) / 2.)
    axe.set_xticklabels(df.index, rotation = 0)
    axe.set_title(title)

    # Add invisible data to add another legend using all algs
    if not clean:
        n=[]        
        for i in range(len(allalgs)):
            chatch = hatches[i]*hatches_mult if hatches[i] else None
            n.append(axe.bar(0, 0, color="gray", hatch=chatch))

        l1 = axe.legend(h[:n_col], l[:n_col], loc=[1.01, 0.7])
        if labels is not None:
            l2 = plt.legend(n, allalgs, loc=[1.01, 0.1],handleheight=2) 
        axe.add_artist(l1)
    
    return axe

count = 0

def plot_bar(ds,work_th,rq_th,u,rq,k,rqsize):
    query = ("SELECT alg AS x, AVG("+metric+") AS y, STDEV("+metric+") AS e" 
             " FROM results" 
             " WHERE ds='"+ds+"'"
                    " AND work_th="+work_th+
                    " AND rq_th="+rq_th+
                    " AND ins="+u+
                    " AND del="+u+
                    " AND rq="+rq+ 
                    " AND max_key="+k+
                    " AND rq_size="+rqsize+
                    " AND alg!='unsafe'"
             " GROUP BY x ORDER BY x")
    state = ["k"+k,"u"+u,"rq"+rq,"rqsize"+rqsize,"nwork"+work_th,"nrq"+rq_th]
    title = dsToName[ds]
    if not clean:
        title += "\n" + " ".join(state)
    df = pd.read_sql(query, conn)
    df = df.set_index('x')
    df = df.reindex(index=dsToAlgs[ds])
    plt.figure()
    ax = df.plot(kind='bar', y='y',legend=False, color='white', edgecolor='black', linewidth=2, title=title)
    plt.xticks(rotation=45)
    plt.xlabel("")
    plt.title(title,fontsize = 24)
    plt.yticks(fontsize = 22)
    filename = "bar-"+metric_name+"-"+ds+"-"
    filename += "-".join(state)
    ymin,ymax = ax.get_ylim()
    if ds == "skiplistlock" : 
      ax.set_aspect(0.04)
      plt.yticks(np.arange(ymin, ymax, 10))
    if ds == "lflist" : 
      ax.set_aspect(1.4)
      plt.yticks(np.arange(ymin, ymax, 0.25))
    # Loop over the bars
    for i,thisbar in enumerate(ax.patches):
        # Set a different hatch for each bar (add +1 for when using unsafe)
        hatch = dsToHatch[ds][i]
        thisbar.set_hatch(hatch*hatches_mult)
    ax.set_ylim(ymin,ymax+0.1*ymax)
    if clean:
        plt.xticks([], [])
    ax.grid(color='grey', linestyle='-')
    if not clean: 
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),handleheight=2)
        plt.savefig(outdir+filename+".png", bbox_extra_artists=(ax.get_legend(),), bbox_inches='tight')
    else:
        plt.savefig(outdir+filename+".png",bbox_inches='tight')
    plt.close('all')

def plot_stacked_bar(ds,work_th,rq_th,u,rq,k,rqsize):
    query = ("SELECT alg AS x, AVG("+metric1+") AS "+metric1_name+", AVG("+metric2+") AS "+metric2_name+" , AVG("+metric3+") AS "+metric3_name+ 
        " FROM results" 
        " WHERE ds='"+ds+"'"
               " AND work_th="+work_th+
                " AND rq_th="+rq_th+
                " AND ins="+u+
                " AND del="+u+
                " AND rq="+rq+ 
                " AND max_key="+k+
                " AND rq_size="+rqsize+
                " AND alg!='unsafe'"
        " GROUP BY x ORDER BY x")
    state = ["k"+k,"u"+u,"rq"+rq,"rqsize"+rqsize,"nwork"+work_th,"nrq"+rq_th]
    title = dsToName[ds]
    if not clean:
        title += "\n"+" ".join(state)
    df = pd.read_sql(query, conn)
    plt.figure()
    if not clean:
        df.plot(kind='bar',stacked=True, x='x', y=[metric1_name,metric2_name,metric3_name], title=title, edgecolor='white')
    else: 
        df.plot(kind='bar',stacked=True, legend=False, x='x', y=[metric1_name,metric2_name,metric3_name], title=title, edgecolor='white')
    plt.xticks(rotation=45)
    plt.xlabel("")
    filename = "stacked_bar"+"-"+ds+"-"
    filename += "-".join(state)
    plt.savefig(outdir+filename+".png",bbox_inches='tight')
    plt.close('all')
    
    #add a graph with unsafe 
    query = ("SELECT alg AS x, AVG("+metric1+") AS "+metric1_name+", AVG("+metric2+") AS "+metric2_name+" , AVG("+metric3+") AS "+metric3_name+ 
        " FROM results" 
        " WHERE ds='"+ds+"'"
               " AND work_th="+work_th+
                " AND rq_th="+rq_th+
                " AND ins="+u+
                " AND del="+u+
                " AND rq="+rq+ 
                " AND max_key="+k+
                " AND rq_size="+rqsize+
        " GROUP BY x ORDER BY x")
    df = pd.read_sql(query, conn)
    plt.figure()
    if not clean:
        df.plot(kind='bar',stacked=True, x='x', y=[metric1_name,metric2_name,metric3_name], title=title, edgecolor='white')
    else: 
        df.plot(kind='bar',stacked=True, legend=False, x='x', y=[metric1_name,metric2_name,metric3_name], title=title, edgecolor='white')
    plt.xticks(rotation=45)
    plt.xlabel("")
    plt.savefig(outdir+"unsafe/"+filename+".png",bbox_inches='tight')
    plt.close('all')

def plot_series_bar(series,where,filename,user_title,xlabel):
    query = ("SELECT " + series + " AS series, alg AS Algorithm, AVG("+metric+") AS y"
             " FROM results" 
             +where+
                " AND alg!='unsafe'"
             " GROUP BY series, Algorithm" +
             " ORDER BY series, Algorithm")
    title = dsToName[ds]
    if not clean:
        title += "\n"+user_title
    df = pd.read_sql(query, conn)
    df = df.pivot(index='series', columns='Algorithm', values='y')
    df = df.reindex(columns=dsToAlgs[ds])
    fig = plt.figure()
    if not clean:
        ax = df.plot(kind='bar', legend=True, color='white', edgecolor='black', linewidth=2,  width=0.75, title=title)
        plt.xlabel(xlabel)
    else:
        ax = df.plot(kind='bar', legend=False, color='white', edgecolor='black', linewidth=2,  width=0.75, title=title)
        plt.xlabel("")
    
    # Loop over the bars
    for i,thisbar in enumerate(ax.patches):
        # Set a different hatch for each bar (add +1 for when using unsafe)
        hatch = dsToHatch[ds][i/(len(df.index.values))]
        thisbar.set_hatch(hatch*hatches_mult)
        
    plt.xticks(rotation=0, fontsize = 20)
    ymin,ymax = ax.get_ylim()
    ax.set_ylim(ymin,ymax+0.1*ymax)
    ax.grid(color='grey', linestyle='-')
    plt.title(title,fontsize = 24)
    plt.yticks(fontsize = 22)
    if not clean: 
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),handleheight=2)
        plt.savefig(outdir+filename+".png", bbox_extra_artists=(ax.get_legend(),), bbox_inches='tight')
    else:
        plt.savefig(outdir+filename+".png",bbox_inches='tight')
    plt.close('all')
    
def plot_series_line(series,where,filename,user_title,xlabel):
    query = ("SELECT " + series + " AS series, alg AS Algorithm, AVG("+metric+") AS y"
             " FROM results" 
             +where+
                " AND alg!='unsafe'"
             " GROUP BY series, Algorithm" +
             " ORDER BY series, Algorithm")
    title = dsToName[ds]
    if not clean:
        title += "\n"+user_title
    df = pd.read_sql(query, conn)
    if df.empty: 
        print "line graph cannot be created dataframe is empty, ds is "+ds
        print query
        return 
    df = df.pivot(index='series', columns='Algorithm', values='y')
    df = df.reindex(columns=dsToAlgs[ds])
    plt.figure()
    if not clean:
        ax = df.plot(kind='line', legend=True, title=title)
        plt.xlabel(xlabel)              
    else:
        ax = df.plot(kind='line', legend=False, title=title)
        plt.xlabel("") 
    for i, line in enumerate(ax.get_lines()):
            marker = dsToMarker[ds][i]
            line.set_marker(marker) 
            line.set_color(dsToColor[ds][i])
            line.set_linestyle(dsToStyle[ds][i])
            line.set_markeredgecolor("black")
            line.set_linewidth(line_width)
            line.set_markersize(markerToSize[marker])
    if not clean: ax.legend(ax.get_lines(), df.columns, loc='best')
    plt.xticks(df.index.values)
    ymin,ymax = ax.get_ylim()
    ax.set_ylim(0-0.05*ymax,ymax+0.1*ymax)
    xmin,xmax = ax.get_xlim()
    ax.set_xlim(xmin-0.05*xmax,xmax+0.05*xmax)
    plt.xticks(rotation=0)
    ax.grid(color='grey', linestyle='-')
    if not clean: 
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),handleheight=4, handlelength=4)
        plt.savefig(outdir+filename+".png", bbox_extra_artists=(ax.get_legend(),), bbox_inches='tight')
    else:
        plt.savefig(outdir+filename+".png",bbox_inches='tight')
    plt.close('all')
    
    #add a graph with unsafe
    newalgs = dsToAlgs[ds][:]
    newalgs.append("unsafe")
    query = ("SELECT " + series + " AS series, alg AS Algorithm, AVG("+metric+") AS y"
             " FROM results" 
             +where+
             " GROUP BY series, Algorithm" +
             " ORDER BY series, Algorithm")
    df = pd.read_sql(query, conn)
    df = df.pivot(index='series', columns='Algorithm', values='y')
    df = df.reindex(columns=newalgs)
    plt.figure()
    if not clean:
        ax = df.plot(kind='line', legend=True, title=title)
        plt.xlabel(xlabel)              
    else:
        ax = df.plot(kind='line', legend=False, title=title)
        plt.xlabel("") 
    for i, line in enumerate(ax.get_lines()):
            marker = dsToMarker[ds][i]
            line.set_marker(marker) 
            line.set_color(dsToColor[ds][i])
            line.set_linestyle(dsToStyle[ds][i]) 
            line.set_markeredgecolor("black")
            line.set_linewidth(line_width)
            line.set_markersize(markerToSize[marker])
    if not clean: ax.legend(ax.get_lines(), df.columns, loc='best')
    plt.xticks(df.index.values)
    ymin,ymax = ax.get_ylim()
    ax.set_ylim(0-0.05*ymax,ymax+0.1*ymax)
    xmin,xmax = ax.get_xlim()
    ax.set_xlim(xmin-0.05*xmax,xmax+0.05*xmax)
    plt.xticks(rotation=0)
    ax.grid(color='grey', linestyle='-')
    if not clean: 
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),handleheight=4, handlelength=4)
        plt.savefig(outdir+"unsafe/"+filename+".png", bbox_extra_artists=(ax.get_legend(),), bbox_inches='tight')
    else:
        plt.savefig(outdir+"unsafe/"+filename+".png",bbox_inches='tight')
    plt.close('all')

def plot_series_stacked_bar(ds,series,where,filename,user_title,xlabel):
    dfs=[]
    for alg in dsToAlgs[ds]:    
        query = ("SELECT " + series + " AS x, AVG("+metric1+") AS "+metric1_name+", AVG("+metric2+") AS "+metric2_name+ " , AVG("+metric3+") AS "+metric3_name+ 
                " FROM results" 
                +where+
                    " AND alg='"+alg+"'" 
                " GROUP BY x" +
                " ORDER BY x")
        df = pd.read_sql(query, conn, index_col='x')
        if df.empty: 
            print "line graph cannot be created dataframe is empty, ds is "+ds
            return
        dfs.append(df)
    title = dsToName[ds]
    if not clean:
        title += "\n"+user_title
    ax = plot_clustered_stacked(dfs,dsToAlgs[ds],dsToHatch[ds],title)
    if not clean:
        plt.xlabel(xlabel)
        plt.savefig(outdir+filename+".png", bbox_extra_artists=(ax.get_legend(),), bbox_inches='tight')
    else: 
        plt.xlabel("")
        plt.savefig(outdir+filename+".png",bbox_inches='tight')
    plt.close('all')
    
    #add a graph with unsafe
    newalgs = dsToAlgs[ds][:]
    newalgs.append("unsafe")
    query = ("SELECT " + series + " AS x, AVG("+metric1+") AS "+metric1_name+", AVG("+metric2+") AS "+metric2_name+ " , AVG("+metric3+") AS "+metric3_name+ 
                " FROM results" 
                +where+
                    " AND alg='unsafe'" 
                " GROUP BY x" +
                " ORDER BY x")
    df = pd.read_sql(query, conn, index_col='x')
    dfs.append(df)
    ax = plot_clustered_stacked(dfs,newalgs,dsToHatch[ds],title)
    if not clean:
        plt.xlabel(xlabel)
        plt.savefig(outdir+"unsafe/"+filename+".png", bbox_extra_artists=(ax.get_legend(),), bbox_inches='tight')
    else: 
        plt.xlabel("")
        plt.savefig(outdir+"unsafe/"+filename+".png",bbox_inches='tight')
    plt.close('all')
    

def do_dbx_graph():
    df = pd.read_csv('dbx.csv')
    df = df.set_index('alg')
    fig = plt.figure()
    if not clean:
      ax = df.plot(kind='bar', legend=True, color='white', edgecolor='black', linewidth=2,  width=0.75)
    else: 
      ax = df.plot(kind='bar', legend=False, color='white', edgecolor='black', linewidth=2,  width=0.75)
    plt.xlabel("")
    hatches = ["", '.', '/', 'O', 'X','-']
    # Loop over the bars
    for i,thisbar in enumerate(ax.patches):
        # Set a different hatch for each bar (add +1 for when using unsafe)
        hatch = hatches[i/(len(df.index.values))]
        thisbar.set_hatch(hatch*hatches_mult)
    if not clean: ax.legend(ax.get_lines(), df.columns, loc='best')   
    plt.xticks(rotation=0, fontsize = 14)
    ax.grid(color='grey', linestyle='-')
    plt.yticks(fontsize = 12)
    ax.set_aspect(1.5)
    ymin,ymax = ax.get_ylim()
    plt.yticks(np.arange(ymin, ymax, 0.2))
    ax.xaxis.grid(which='major', linewidth=0)
    if not clean:
      ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),handleheight=2, handlelength=2)
      plt.savefig(outdir+"dbx.png",bbox_extra_artists=(ax.get_legend(),),bbox_inches='tight')
    else:
      plt.savefig(outdir+"dbx.png",bbox_inches='tight')
    plt.close('all')


print "generating DBX graphs"
do_dbx_graph()

############################################################################
## BASIC MAX-THREAD EXPERIMENTS (rq size 100)
## (varying update rate and data structure size)
############################################################################

print "generating graphs for BASIC MAX-THREAD EXPERIMENTS ... "

rq="0"
rqsize="100"
updates = ["0", "10", "50"]
key_ranges = ["1000", "10000", "100000", "1000000", "10000000"]
nwork_nrqs = [("48","0"),("47","1")]

for ds in datastructures:
    print "generating graphs for " + ds + " ..."
    for u in updates:
        for k in key_ranges:
            for nwork,nrq in nwork_nrqs:
                if not is_valid_key_range(ds,k): continue
#                plot_bar(ds,nwork,nrq,u,rq,k,rqsize)
#                count+=1
#                plot_stacked_bar(ds,nwork,nrq,u,rq,k,rqsize)
#                count+=1


############################################################################
## IMPACT OF INCREASING RQ THREAD COUNT ON UPDATE THREADS (rq size 100)
############################################################################

print "generating graphs for IMPACT OF INCREASING RQ THREAD COUNT ... "
    
rq="0"
rqsize="100"
nwork="42"
if not clean:
    updates = ["10","50"]
else:
    updates = ["50"]
key_ranges = ["10000", "100000", "1000000"]

for ds in datastructures:
    print "generating graphs for " + ds + " ..."
    for k in key_ranges:
        for u in updates:
            if not is_valid_key_range(ds,k): continue
            where = (" WHERE ds='"+ds+"'"
                    " AND work_th="+nwork+
                    " AND ins="+u+
                    " AND del="+u+
                    " AND rq="+rq+ 
                    " AND max_key="+k+
                    " AND rq_size="+rqsize)
            xlabel = "rq threads" 
            state = ["k"+k,"u"+u,"nwork"+nwork,"rq"+rq,"rqsize"+rqsize]
            title_end = " ".join(state)
            filename_end = "-".join(state)

            title = metric_name+"\n"+title_end
            filename = "-".join(("plot_series_line_by_nrq",metric_name,ds))+"-"+filename_end
            plot_series_line("rq_th",where,filename,title,xlabel)
            count+=1

#            filename = "-".join(("plot_series_stacked_bar_by_nrq",ds))+"-"+filename_end
#            plot_series_stacked_bar(ds,"rq_th",where,filename,title_end,xlabel)
#            count+=1


############################################################################
## IMPACT OF INCREASING UPDATE THREAD COUNT ON RQ THREADS (rq size 100)
############################################################################

print "generating graphs for IMPACT OF INCREASING UPDATE THREAD COUNT ... "

updates = ["10","50"]
rq="0"
rqsize="100"
if not clean:
    nrqs=["1", "4"]
else: 
    nrqs=["1"]
key_ranges = ["10000", "100000", "1000000"]
for ds in datastructures:
    print "generating graphs for " + ds + " ..."
    for k in key_ranges:
        for u in updates:
            for nrq in nrqs: 
                if not is_valid_key_range(ds,k): continue
                where = (" WHERE ds='"+ds+"'"
                        " AND rq_th="+nrq+
                        " AND ins="+u+
                        " AND del="+u+
                        " AND rq="+rq+ 
                        " AND max_key="+k+
                        " AND rq_size="+rqsize)
                if clean:
                    where+= " AND work_th IN (1,8,16,24,32,40,47)"
                xlabel = "work threads" 
                state = ["k"+k,"u"+u,"nrq"+nrq,"rq"+rq,"rqsize"+rqsize]       
                title_end = " ".join(state)
                filename_end = "-".join(state)

                title = metric_name+"\n"+title_end
                filename = "-".join(("plot_series_line_by_nwork",metric_name,ds))+"-"+filename_end
                plot_series_line("work_th",where,filename,title,xlabel)
                count+=1

#                filename = "-".join(("plot_series_stacked_bar_by_nwork",ds))+"-"+filename_end
#                plot_series_stacked_bar(ds,"work_th",where,filename,title_end,xlabel)
#                count+=1

############################################################################
## SMALL RQ (k/1000) VS BIG RQ (k/10) (VS ITERATION (rqsize=k))
## (for latency graphs, and for comparison with iterators)
############################################################################
 
print "generating graphs for SMALL RQ (k/1000) VS BIG RQ (k/10) ... "    
    
rq = "0"
if not clean:
    updates = ["10", "50"]
else:
     updates = ["10"]
#key_ranges_rq_sizes = [("10000",["1000","100","10"]), ("100000",["10000","1000","100"]), ("1000000",["1000000","10000","1000"])]
key_ranges = ["10000", "100000", "1000000"]
nrq = "1"
nwork = "47"
for ds in datastructures:
    if clean and ds not in ["bst","citrus","skiplistlock"]: continue
    print "generating graphs for " + ds + " ..."
    for k in key_ranges:
        for u in updates:
            if not is_valid_key_range(ds,k): continue
            where = (" WHERE ds='"+ds+"'"
                    " AND work_th="+nwork+
                    " AND rq_th="+nrq+
                    " AND ins="+u+
                    " AND del="+u+
                    " AND rq="+rq+ 
                    " AND max_key="+k)
            xlabel = "rq size" 
            state = ["k"+k,"u"+u,"nwork"+nwork,"nrq"+nrq,"rq"+rq]
            title_end = " ".join(state)
            filename_end = "-".join(state)
            
            title = metric_name+"\n"+title_end
            filename = "-".join(("plot_series_bar_by_rq_size",metric_name,ds))+"-"+filename_end
            plot_series_bar("rq_size",where,filename,title,xlabel)
            count+=1
            
            filename = "-".join(("plot_series_stacked_bar_by_rq_size",ds))+"-"+filename_end
            plot_series_stacked_bar(ds,"rq_size",where,filename,title_end,xlabel)
            count+=1
    

############################################################################
## MIXED WORKLOAD WHERE ALL THREADS DO UPDATES AND RQs (rqsize=100)
############################################################################

print "generating graphs for MIXED WORKLOAD ... "

rqsize = "100"
rq = "2"
u = "10"
key_ranges = ["10000", "100000", "1000000"]
nrq = "0"
nwork = "48"
for ds in datastructures:
    print "generating graphs for " + ds + " ..."
    for k in key_ranges:
        if not is_valid_key_range(ds,k): continue
        plot_bar(ds,nwork,nrq,u,rq,k,rqsize)
        count+=1
#        plot_stacked_bar(ds,nwork,nrq,u,rq,k,rqsize)
#        count+=1
                

print "Generated " + str(count) + " graphs"
conn.close()
