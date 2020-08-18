"""
Script that plots benchmark data-visualizations.
"""
import gzip
import urllib.request
from plotnine.themes.theme_gray import theme_gray
from plotnine.themes.theme import theme
from plotnine.themes.elements import (element_line, element_rect,
                                      element_text, element_blank)
import sys
import pandas as pd
import numpy as np
import plotnine as p9
from plotnine import *
from plotnine.data import *
import warnings
from io import BytesIO

# What machine, max cores, sockets, revision
MACHINES = [('skylake2x', 56, 2),
            ('skylake4x', 192, 4)]

def get_gzip_csv_data(machine):
    url = "https://gz.github.io/node-replication/scalebench/{}/{}/scaleout_benchmarks.csv.gz".format(
        machine[0], machine[3])
    print(url)
    response = urllib.request.urlopen(url)
    buf = BytesIO(response.read())
    return gzip.GzipFile(fileobj=buf)
def get_csv_data(machine):
    url = "https://gz.github.io/node-replication/scalebench/{}/{}/scaleout_benchmarks.csv".format(
        machine[0], machine[3])
    return urllib.request.urlopen(url)
class theme_my538(theme_gray):
    def __init__(self, base_size=6, base_family='DejaVu Sans'):
        theme_gray.__init__(self, base_size, base_family)
        bgcolor = '#FFFFFF'
        self.add_theme(
            theme(
                strip_margin=0,
                strip_margin_x=0,
                strip_margin_y=0,
                legend_box_margin=0,
                legend_margin=0,
                axis_text=element_text(size=base_size),
                axis_ticks=element_blank(),
                title=element_text(color='#3C3C3C'),
                legend_background=element_rect(fill='None'),
                legend_key=element_rect(fill='#FFFFFF', colour=None),
                panel_background=element_rect(fill=bgcolor),
                panel_border=element_blank(),
                panel_grid_major=element_line(
                    color='#D5D5D5', linetype='solid', size=0.5),
                panel_grid_minor=element_blank(),
                panel_spacing=0.15,
                plot_background=element_rect(
                    fill=bgcolor, color=bgcolor, size=1),
                strip_background=element_rect(size=0)),
            inplace=True)

def throughput_vs_cores(df, write_ratios=[100]):
    data_set = []
    machine = []
    for name in df.name.unique():
        benchmark = df.loc[df['name'] == name]
        _dtname, dtname, _ignore, write_ratio = name.split("-")
        write_ratio = int(write_ratio[2:])
        cores = benchmark.threads.max()
        if cores == int('192'):
            machine = MACHINES[1]
        else:
            machine = MACHINES[0]
        #print(dtname, write_ratio)
        assert(len(benchmark.duration.unique()) == 1)

        if write_ratio not in write_ratios:
            continue
        benchmark['dtname'] = dtname
        benchmark['write_ratio'] = write_ratio
        benchmark = benchmark.groupby(['name', 'dtname', 'write_ratio', 'rs', 'tm', 'batch_size', 'threads', 'duration'], as_index=False).agg(
            {'exp_time_in_sec': 'max', 'iterations': 'sum'})
        benchmark['throughput'] = benchmark['iterations'] / benchmark['exp_time_in_sec']
        benchmark['configuration'] = benchmark.apply(
            lambda row: "RS={} TM={} BS={}".format(row.rs, row.tm, row.batch_size), axis=1)
        #print(benchmark)
        data_set.append(benchmark)
    benchmarks = pd.concat(data_set)

    xskip = int(machine[1]/8)
    p = ggplot(data=benchmarks,
               mapping=aes(x='threads',
                           y='throughput',
                           color='dtname',
                           shape='dtname')) + \
        theme_my538() + \
        coord_cartesian(ylim=(0, None), expand=False) + \
        labs(y="Throughput [Melems/s]") + \
        theme(legend_position='top', legend_title=element_blank()) + \
        scale_x_continuous(breaks=[1] + list(range(xskip, 513, xskip)), name='# Threads') + \
        scale_y_continuous(labels=lambda lst: ["{:,.0f}".format(x / 1_000_000) for x in lst]) + \
        geom_point() + \
        geom_line() + \
        facet_grid(["write_ratio", "."], scales="free_y") + \
        guides(color=guide_legend(nrow=1))
    sockets = machine[2]
    threads_per_socket = machine[1] / machine[2]
    annotation_data = []
    for wr in benchmarks['write_ratio'].unique():
        ht = 0
        for hts in range(0, 2):
            for s in range(0, sockets):
                ht += threads_per_socket / 2
                lt = ('B', 'A')[ht == machine[1] / 2]
                annotation_data.append(
                    [wr, ht, max(benchmarks.loc[benchmarks['write_ratio'] == wr]['throughput']), lt])
    annotations = pd.DataFrame(annotation_data, columns=[
                               'write_ratio', 'threads', 'yend', 'lt'])
    p += geom_segment(data=annotations,
                      mapping=aes(x='threads', xend='threads',
                                  y=0, yend='yend', linetype='lt'),
                      color='black')
    p += scale_linetype_manual(values=['dashed', 'dotted'],
                               guide=None)
    p.save("throughput-{}-wr{}.png".format(machine[0], wr), dpi=300)
    p.save("throughput-{}-wr{}.pdf".format(machine[0], wr), dpi=300)

if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    pd.set_option('display.expand_frame_repr', True)
    if len(sys.argv) == 2:
        df = pd.read_csv(sys.argv[1], skip_blank_lines=True)
        throughput_vs_cores(df.copy())
    else:
        for machine in MACHINES:
            df = pd.read_csv(get_gzip_csv_data(machine), skip_blank_lines=True)
            throughput_vs_cores(machine, df.copy())
