import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import numpy as np
import itertools
import fileinput
from timeit import default_timer as timer

class TimelinePlotter:
    outfile="timeline_output.png"
    maxIntervals=100000  ## soft cap on total number of intervals (to limit render time without confirmation / override)
    scale=1000000 # nanos to millis
    mintime_ns=-1
    maxtime_ns=0
    windowEnd_ms=0
    start_split=0
    last_split=0

    def split(self):
        curr_split = timer()
        diff = curr_split - self.last_split
        difftotal = curr_split - self.start_split
        print("elapsed %.1fs (total %.1fs)" % (diff, difftotal))
        self.last_split = curr_split

    def __init__(self, settings):
        self.windowEnd_ms = settings.windowStart_ms + settings.windowSize_ms
        self.start_split = timer()
        self.last_split = timer()

        ## compute min and max start time
        for line in fileinput.input():
            start = settings.getStart(line)
            end = settings.getEnd(line)

            if self.mintime_ns == -1 or self.mintime_ns > start:
                self.mintime_ns = start
            if self.maxtime_ns < start:
                self.maxtime_ns = start

        print "found min time " + repr(self.mintime_ns) + " and max time " + repr(self.maxtime_ns) + " duration " + repr((self.maxtime_ns - self.mintime_ns) / self.scale) + " ms"
        self.split()

        ## build data frame (process all lines)
        ThreadCol = []
        StartCol = []
        FinishCol = []
        ColorCol = []
        lendf = 0

        colorit=itertools.cycle(cm.rainbow(np.linspace(0, 1, 10))) ## 10 colors picked linearly through cm.rainbow

        # df = []
        count=0
        for line in fileinput.input():
            if settings.includeLine(line) == True:
                thr = settings.getThread(line)
                start = settings.getStart(line) - self.mintime_ns
                end = settings.getEnd(line) - self.mintime_ns
                duration = end - start

                if duration >= settings.minDuration_ms*self.scale:
                    if thr <= settings.maxThread:
                        if (start >= settings.windowStart_ms*self.scale and start <= self.windowEnd_ms*self.scale) or (end >= settings.windowStart_ms*self.scale and end <= self.windowEnd_ms*self.scale):
                            # df.append(dict({'Task': int(thr), 'Start': long(start), 'Finish': long(end), 'Color': settings.getColor(line)}))

                            lendf = lendf + 1
                            ThreadCol.append(int(thr))
                            StartCol.append(long(start))
                            FinishCol.append(long(end))

                            #ColorCol.append(settings.getColor(line))

                            c = next(colorit)
                            ColorCol.append(c)

        # lendf = len(df)
        # print "built data frame; len=" + repr(lendf)
        self.split()

        good = True
        if lendf > self.maxIntervals:
            printf("WARNING: over {} intervals detected... it's likely that THIS many printed events affected your performance!!!".format(self.maxIntervals))
            yesno = raw_input("Large number of intervals... Do you still want to build the graph [y/n]? ")
            if yesno == 'y' or yesno == 'Y':
                good = True
            else:
                good = False

        if good:
            plt.style.use('dark_background')
            height_inches = 12
            width_inches = 40
            dots_per_inch = 200
            plt.figure(figsize=(width_inches, height_inches), dpi=dots_per_inch)

            self.split()
            # print "start drawing hlines"
            # lineno=0
            # for row in df:
            #     lineno = lineno + 1
            #     if (lineno % 1000) == 0:
            #         print "line number " + repr(lineno)
            #         self.split()

            #     plt.hlines(int(row["Task"]), long(row["Start"]), long(row["Finish"]), lw=1, colors=[row["Color"]])
            #     ## https://datascience.stackexchange.com/questions/33237/how-to-create-a-historical-timeline-using-pandas-dataframe-and-matplotlib
            # print "finished drawing hlines"

            # numUniqueThreads = len(set(ThreadCol))
            # print("numUniqueThreads = {}".format(numUniqueThreads))

            threadRange = max(ThreadCol) - min(ThreadCol) + 1
            print("threadRange = {}".format(threadRange))

            lwidth = max(1, (height_inches * dots_per_inch) / 5 / threadRange)
            print('automatic lwidth = {}'.format(lwidth))

            plt.hlines(ThreadCol, StartCol, FinishCol, ColorCol, linewidth=lwidth)

            self.split()

            print("saving figure %s\n" % self.outfile)
            plt.tight_layout()

            plt.savefig(self.outfile)
            self.split()

            # yesno = raw_input("Do you want to show the figure now using X11 [y/n]? ")
            # if yesno == 'y' or yesno == 'Y':
            #     print "showing figure..."
            #     plt.show()
            # else:
            #     print "done."

        self.split()
