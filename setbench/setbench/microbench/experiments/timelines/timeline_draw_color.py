## customize the following settings for your input.
## by default, this expects four space-separated columns:
##     thread_number start_time_in_nanosec end_time_in_nanosec color
##
## where color is chosen from:
## https://matplotlib.org/gallery/color/named_colors.html

class Settings:
    maxThread = 1000        ## max thread for which to render intervals
    windowStart_ms = 0      ## only render intervals starting from x millis
    windowSize_ms = 60000   ## only render intervals for y millis after windowStart_ms
    minDuration_ms = 20     ## min duration required for an interval to be rendered

    ## should return the thread number for a given line (interval)
    def getThread(self, line):
        return long(line.split(" ")[0])

    ## should return the interval start for a given line (interval)
    def getStart(self, line):
        return long(line.split(" ")[1])

    ## should return the interval end for a given line (interval)
    def getEnd(self, line):
        return long(line.split(" ")[2])

    ## should return the color for a given line (interval)
    def getColor(self, line):
        return line.split(" ")[3].rstrip()

    ## should return True if the line is to be included in the graph
    ## and False otherwise
    def includeLine(self, line):
        return True

from timeline_plotter import TimelinePlotter
plot = TimelinePlotter(Settings())
