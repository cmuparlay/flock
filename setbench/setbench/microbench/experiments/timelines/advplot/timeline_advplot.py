## todo:
##  - allow user to specify desired limit for NUMBER of events,
##    and prune by duration to hit the limit
##  - more gradient color rendering
##  - option for gradient to represent heatmap values based on numeric data "labels"
##  - todo: make "total" series optional (it tracks blip events)

import numpy as np
import itertools
import fileinput
from timeit import default_timer as timer
import sys, getopt
import pickle
import platform

SHOW = (len(sys.argv) >= 3 and sys.argv[2] == '__show__')
print("SHOW={}".format(SHOW))

import matplotlib as mpl
if platform.system() == 'Windows' or 'CYGWIN' in platform.system():
    mpl.use('TkAgg')
else:
    mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
from matplotlib.collections import PatchCollection

class Settings:
    infile=""
    outfile=""
    pairs=[]
    events_short=[]
    eventsShortToLong=dict()

    maxIntervals=150000     ## soft cap on total number of intervals (to limit render time without confirmation / override)
    maxThread = 1000        ## max thread for which to render intervals
    windowStart_ms = 0      ## only render intervals starting from x millis
    windowSize_ms = 60000   ## only render intervals for y millis after windowStart_ms
    minDuration_ms = 1      ## min duration required for an interval to be rendered

    eventToColor=dict()
    colorit=itertools.cycle(cm.rainbow(np.linspace(0, 1, 10))) ## 10 colors picked linearly through cm.rainbow

    def getEvent(self, line):
        return line.split(" ")[0]

    ## should return the thread number for a given line (interval)
    def getThread(self, line):
        return int(line.split(" ")[1])

    ## should return the interval start for a given line (interval)
    def getStart(self, line):
        return int(line.split(" ")[2])

    ## should return the interval end for a given line (interval)
    def getEnd(self, line):
        if 'blip_' in self.getEvent(line):
            return -1 ## indicate this is a blip
        else:
            return int(line.split(" ")[3])

    ## should return the color for a given line (interval)
    def getColor(self, line):
        e = self.getEvent(line)
        c = self.eventToColor[e]
        if c == 'rainbow':
            return next(self.colorit)
        else:
            return c

    def getLabel(self, line):
        if 'blip_' in self.getEvent(line): ## handle case where datapoint is a blip
            if len(line.split(" ")) < 4:
                return None
            else:
                return line.split(" ")[3]
        else:
            if len(line.split(" ")) < 5:
                return None
            else:
                return line.split(" ")[4]

    ## should return True if the line is to be included in the graph
    ## and False otherwise
    def includeLine(self, line):
        return self.getEvent(line) in self.eventsShortToLong.values()
        # return self.getEvent(line) in self.events

    def __init__(self, infile, outfile, pairs):
        self.infile = infile
        self.outfile = outfile
        self.pairs = pairs
        ## parse mapping from event names to colors
        for i in range(len(pairs)//2):
            self.events_short.append(pairs[2*i])

        # print(pairs)
        # print(self.eventToColor)
        # print(self.events_short)

        ## build eventsShortToLong dictionary
        for line in fileinput.input(infile):
            line = line.rstrip('\r\n')
            line_event = self.getEvent(line)
            if line_event not in self.eventsShortToLong.keys():
                for short_event in self.events_short:
                    if short_event in line_event:
                        self.eventsShortToLong[short_event] = line_event

        # print()
        # print('####')
        # print(self.events_short)
        # print()
        # print('####')
        # print(self.eventsShortToLong)

        for i in range(len(pairs)//2):
            short_event = pairs[2*i]
            event_color = pairs[2*i+1]
            if short_event not in self.eventsShortToLong.keys():
                print()
                print("############")
                print("## Warning: event {} does not appear in the input".format(short_event))
                print("############")
                print()
            else:
                self.eventToColor[self.eventsShortToLong[short_event]] = event_color

## configurable parameters
scale=1000000               ## nanos to millis
windowEnd_ms=-1
mintime_ns=-1
maxtime_ns=0                ## initialized for us
start_split=0
last_split=0

width = -1
height = -1

def split():
    global last_split

    curr_split = timer()
    diff = curr_split - last_split
    difftotal = curr_split - start_split
    print("elapsed %.1fs (total %.1fs)" % (diff, difftotal))
    last_split = curr_split

if len(sys.argv) < 4:
    print('USAGE: python timeline_advplot.py <infile> <outfile> <event> <color> [[<event> <color>] ...]')
    print('       <infile> should contain (only) lines w/format "<event> <thread_id> <start_time> <end_time>"')
    print('       <outfile> is an image to create (with desired extension, e.g., .png or .svg)')
    print('       <event> should be an event name present in <infile>')
    print('       <color> can be any named python color or "rainbow"')
    print()
    print(' Note: __show__ can be specified as <outfile> to launch an interactive figure window instead.')
    exit(1)

print(sys.argv)
settings = Settings(infile=sys.argv[1], outfile=sys.argv[2], pairs=sys.argv[3:])

windowEnd_ms = settings.windowStart_ms + settings.windowSize_ms
start_split = timer()
last_split = timer()

## build

## compute min and max start time
# print(settings)
for line in fileinput.input(settings.infile):
    line = line.rstrip('\r\n')
    start = settings.getStart(line)
    end = settings.getEnd(line)

    if mintime_ns == -1 or mintime_ns > start:
        mintime_ns = start
    if maxtime_ns < end:
        maxtime_ns = end
    if maxtime_ns < start:
        maxtime_ns = start

print("found min time {} and max time {} duration {} ms".format(mintime_ns, maxtime_ns, (maxtime_ns - mintime_ns) / scale))
split()

## build data frame (process all lines)
EventCol = []
ThreadCol = []
StartCol = []
FinishCol = []
ColorCol = []
LabelCol = []
count = 0

countmap_accepted = dict()
countmap_rejected_duration = dict()

for line in fileinput.input(settings.infile):
    line = line.rstrip('\r\n')
    if settings.includeLine(line) == True:
        e = settings.getEvent(line)
        if e not in countmap_accepted.keys(): countmap_accepted[e] = 0
        if e not in countmap_rejected_duration.keys(): countmap_rejected_duration[e] = 0

        thr = settings.getThread(line)
        start = settings.getStart(line) - mintime_ns
        end = settings.getEnd(line)
        # if 'blip' in line: print('line {} end {}'.format(line, end))
        if end != -1: end = end - mintime_ns
        duration = end - start

        # print(line)

        if duration >= settings.minDuration_ms*scale or end == -1:
            if thr <= settings.maxThread:
                if (start >= settings.windowStart_ms*scale and start <= windowEnd_ms*scale) or (end >= settings.windowStart_ms*scale and end <= windowEnd_ms*scale):
                    count = count + 1
                    EventCol.append(e)
                    ThreadCol.append(int(thr))
                    StartCol.append(int(start))
                    FinishCol.append(int(end))
                    ColorCol.append(settings.getColor(line))
                    LabelCol.append(settings.getLabel(line))

                    countmap_accepted[e] = countmap_accepted[e] + 1
                else: print('rejecting {} because of start {} or end {} vs windowStart {} or windowEnd{} or scale {}'.format(line, start, windowStart_ms, windowEnd_ms, scale))
            else: print('rejecting {} because of thread {}'.format(line, thr))
        else:
            # print('rejecting {} because of duration {} given start {} and end {}'.format(line, duration, start, end))
            countmap_rejected_duration[e] = countmap_rejected_duration[e] + 1
    else: print('rejecting {} because of includeLine:False'.format(line))

print()
print("Added the following events...")
print(countmap_accepted)
print()

for e in countmap_rejected_duration.keys():
    v = countmap_rejected_duration[e]
    if v > 0:
        print()
        print("################################################################################")
        print("## warning: rejected {} of {} due to duration < {}ms ({}% total)".format(v, e, settings.minDuration_ms, 100*(v / (v+countmap_accepted[e]))))
        print("################################################################################")
        print()

split()

if count == 0:
    print("No matching lines found. Check event names.")
    exit(1)

good = True
if count > settings.maxIntervals:
    print("WARNING: over {} intervals detected... it's likely that THIS many printed events affected your performance!!!".format(settings.maxIntervals))
    yesno = input("Large number of intervals... Do you still want to build the graph [y/n]? ")
    # yesno = raw_input("Large number of intervals... Do you still want to build the graph [y/n]? ")
    if yesno == 'y' or yesno == 'Y':
        good = True
    else:
        good = False

if not good:
    print("skipping render...")
    exit(0)

plt.style.use('dark_background')

if SHOW:
    if platform.system() == 'Windows' or platform.system().contains("CYGWIN"):
        # from win32api import GetSystemMetrics
        # screenwidth = GetSystemMetrics(0)
        # screenheight = GetSystemMetrics(1)
        import ctypes
        user32 = ctypes.windll.user32
        user32.SetProcessDPIAware()
        [screenwidth, screenheight] = [user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)]
        print("platform {} detected... screen dimensions {} {}".format(platform.system(), screenwidth, screenheight))
    else:
        # how to do this on linux?
        screenwidth=1920
        screenheight=1080 # fairly good guess for now ...

    dots_per_inch = 72
    width_inches = int(screenwidth*0.98/dots_per_inch)
    height_inches = int(screenheight*0.9/dots_per_inch)
else:
    global fig

    dots_per_inch = 200
    height_inches = 20
    width_inches = 40

fig = plt.figure(figsize=(width_inches, height_inches), dpi=dots_per_inch)
print(dots_per_inch, width_inches, height_inches)

annot = plt.annotate("", xy=(0,0), xytext=(20,20),textcoords="offset points",
                    bbox=dict(boxstyle="round", fc="w"),
                    arrowprops=dict(arrowstyle="->"))
annot.set_visible(False)

split()

miny = min(ThreadCol)
maxy = max(ThreadCol)
numThreads = maxy - miny + 1
print("numThreads = {}".format(numThreads))

minx = min(StartCol)
maxx = max(max(StartCol), max(FinishCol))

lwidth = max(1, int((height_inches * dots_per_inch) / 5 / numThreads))
print('automatic lwidth = {}'.format(lwidth))

print([minx, maxx], [miny, maxy])

if SHOW:
    mng = plt.get_current_fig_manager()
    mng.window.state('zoomed')

    plt.xlim([minx, maxx])
    plt.ylim([miny-1, maxy+1])
    plt.tight_layout()
    ax = plt.gca()
    # print(ax.transData.transform((0,1))-ax.transData.transform((0,0)))

ax = plt.gca()
patches_over = []
patches = []
yboxfrac = 0.7
yscaler = maxy-miny+2
xscaler = maxx-minx
proportional_width = (yboxfrac / yscaler) * xscaler * (height_inches / width_inches)
print(proportional_width, yboxfrac, xscaler, yscaler)

for e, t, s, f, c, l in zip(EventCol, ThreadCol, StartCol, FinishCol, ColorCol, LabelCol):
    ## if this is a blip, not an interval
    if f == -1:
        xo = (proportional_width / 2) * 0.75 ## to make the diamonds diamondey
        yo = yboxfrac / 2
        patches_over.append(mpl.patches.Polygon(np.array([[s-xo, t], [s, t+yo], [s+xo, t], [s, t-yo]], np.float64), True, color=c, ec='none'))
        # patches_over.append(mpl.patches.Ellipse((s, t), proportional_width, yboxfrac, color=c, ec='none'))
        # patches_over.append(mpl.patches.Rectangle((s, t), proportional_width, yboxfrac, color=c, ec='none', angle=45))

        ## extra patch for a "totals" curve at -1
        patches_over.append(mpl.patches.Polygon(np.array([[s-xo, -1], [s, -1+yo], [s+xo, -1], [s, -1-yo]], np.float64), True, color=c, ec='none'))

    ## it is an interval
    else:
        patches.append(mpl.patches.Rectangle((s, t-0.5*yboxfrac), f-s, yboxfrac, color=c, ec='none'))

ax.add_collection(PatchCollection(patches, match_original=True, zorder=0))
ax.add_collection(PatchCollection(patches_over, match_original=True, zorder=1))

# plt.hlines(ThreadCol, StartCol, FinishCol, ColorCol, linewidth=lwidth)

if False:
    for e, t, s, f, c, l in zip(EventCol, ThreadCol, StartCol, FinishCol, ColorCol, LabelCol):
        if l != None and l != "" and t < 10:
            plt.text(s, t, str(l), ha='left', va='center', fontsize=max(12,lwidth), bbox=dict(boxstyle='square,pad=0', facecolor='black', edgecolor='none', alpha=0.7))

# def update_annot(ind):
#     pos = sc.get_offsets()[ind["ind"][0]]
#     annot.xy = pos
#     text = "{}, {}".format(" ".join(list(map(str,ind["ind"]))),
#                             " ".join([names[n] for n in ind["ind"]]))
#     annot.set_text(text)
#     annot.get_bbox_patch().set_facecolor(cmap(norm(c[ind["ind"][0]])))
#     annot.get_bbox_patch().set_alpha(0.4)

# def hover(event):
#     print(event)
    # vis = annot.get_visible()
    # if event.inaxes == ax:
    #     cont, ind = sc.contains(event)
    #     if cont:
    #         update_annot(ind)
    #         annot.set_visible(True)
    #         fig.canvas.draw_idle()
    #     else:
    #         if vis:
    #             annot.set_visible(False)
    #             fig.canvas.draw_idle()

# fig.canvas.mpl_connect("motion_notify_event", hover)

# def resize(event):
#     print(event.width, event.height)

# fig.canvas.mpl_connect("resize_event", resize)

split()

plt.tight_layout()

if SHOW:
    print("showing object")
    plt.show()
elif settings.outfile.endswith(".pickle"):
    print("saving pickle object %s\n" % settings.outfile)
    f = file(settings.outfile, 'w')
    pickle.dump(fig, f)
    f.close()
else:
    print("saving figure image %s\n" % settings.outfile)
    plt.savefig(settings.outfile)

split()
