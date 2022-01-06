#!/bin/python3

## so whatever matplotlib plot styling you like here, to be performed just after importing mpl
def style_init(mpl):
    plt = mpl.pyplot

    mpl.rcParams['figure.autolayout'] = True
    mpl.rcParams['hatch.color'] = 'k'
    mpl.rcParams['hatch.linewidth'] = '3'
    mpl.rcParams['font.size'] = 12
    mpl.rcParams['figure.dpi'] = 72
    plt.style.use('dark_background')
    return

## do whatever matplotlib plot styling you like here, to be performed BEFORE plotting
def style_before_plotting(mpl, plot_kwargs_dict, legend_kwargs_dict=None):
    plt = mpl.pyplot
    fig = plt.gcf()
    ax = plt.gca()

    ## if you want to know what settings are already set (so you can override them)
    ## then print the plotting parameters as follows:
    print('plot_kwargs_dict={}'.format(plot_kwargs_dict))
    if legend_kwargs_dict:
        print('legend_kwargs_dict={}'.format(legend_kwargs_dict))

    ## note: by default this printing will NOT appear in stdout, because it is redirected to output_log.txt!
    ## so look there, to see the result of printing...
    ## (you can also look in plotbars.py at the dict that is passed to this function.)

    ## for example, we will override the plot size (in inches)
    plot_kwargs_dict['figsize'] = (7, 4.5) ## w, h

    ## we will also override the number of columns in the legend
    if legend_kwargs_dict:
        legend_kwargs_dict['ncol'] = 2

    return

## do whatever matplotlib plot styling you like here, to be performed AFTER plotting
def style_after_plotting(mpl):
    plt = mpl.pyplot
    fig = plt.gcf()
    ax = plt.gca()

    ##
    ## examples of some of the things you can do:
    ##

    # bars = ax.patches
    # patterns =( 'x', '/', '//', 'O', 'o', '\\', '\\\\', '-', '+', ' ' )
    # hatches = [p for p in patterns for i in range(len(bars))]
    # for bar, hatch in zip(bars, hatches):
    #     bar.set_hatch(hatch)

    # plt.grid(axis='y', which='major', linestyle='-')
    # plt.grid(axis='y', which='minor', linestyle='--')

    # import matplotlib.ticker as ticker
    # ax.yaxis.set_minor_locator(ticker.LogLocator(subs="all"))
    # ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())

    # plt.setp(ax.get_xticklabels(), visible=False)
    # plt.setp(ax.get_yticklabels(), visible=False)

    # plt.legend(title=None, loc='upper center', bbox_to_anchor=(0.5, -0.05), fancybox=True, shadow=True, ncol=args.legend_columns)

    return
