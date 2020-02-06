#!/usr/bin/env python

import csv
import os
import scipy
import sys

infile = sys.argv[1]
X = scipy.load(infile)

colors = None
if len(sys.argv) > 2:
    labels = {}
    for fnm in os.listdir(sys.argv[3]):
        if '.2.fasta' not in fnm:
            labels[fnm] = '---'
    if len(labels) != len(X):
        raise "len(inputdir) != len(inputarray)"
    with open(sys.argv[2], 'rb') as csvfile:
        for row in csv.reader(csvfile):
            ident=row[0]
            label=row[1]
            for fnm in labels:
                if row[0] in fnm:
                    labels[fnm] = row[1]
    colors = []
    labelcolors = {
        'PUR': 'firebrick',
        'CLM': 'firebrick',
        'MXL': 'firebrick',
        'PEL': 'firebrick',
        'TSI': 'green',
        'IBS': 'green',
        'CEU': 'green',
        'GBR': 'green',
        'FIN': 'green',
        'LWK': 'coral',
        'MSL': 'coral',
        'GWD': 'coral',
        'YRI': 'coral',
        'ESN': 'coral',
        'ACB': 'coral',
        'ASW': 'coral',
        'KHV': 'royalblue',
        'CDX': 'royalblue',
        'CHS': 'royalblue',
        'CHB': 'royalblue',
        'JPT': 'royalblue',
        'STU': 'blueviolet',
        'ITU': 'blueviolet',
        'BEB': 'blueviolet',
        'GIH': 'blueviolet',
        'PJL': 'blueviolet',
    }
    for fnm in sorted(labels.keys()):
        colors.append(labelcolors[labels[fnm]])

from matplotlib.figure import Figure
from matplotlib.patches import Polygon
from matplotlib.backends.backend_agg import FigureCanvasAgg
fig = Figure()
ax = fig.add_subplot(111)
ax.scatter(X[:,0], X[:,1], c=colors, s=60, marker='o', alpha=0.5)
canvas = FigureCanvasAgg(fig)
canvas.print_figure(infile+".png", dpi=80)
