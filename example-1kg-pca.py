#!/usr/bin/env python

"""
lightning gvcf2numpy -tag-library ~/keep/by_id/su92l-4zz18-92bx4zjg5hgs3yc/tagset.fa.gz -ref ./hg38.fa.gz ~/keep/by_id/su92l-4zz18-s3e6as6uzsoocsb > example.npy
"""

import sys
infile = sys.argv[1]

import scipy
X = scipy.load(infile)

from sklearn.decomposition import PCA
pca = PCA(n_components=4)
X = pca.fit_transform(X)

from matplotlib.figure import Figure
from matplotlib.patches import Polygon
from matplotlib.backends.backend_agg import FigureCanvasAgg
fig = Figure()
ax = fig.add_subplot(111)
ax.scatter(X[:,0], X[:,1])
canvas = FigureCanvasAgg(fig)
canvas.print_figure(infile+".png", dpi=80)
