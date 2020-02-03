#!/usr/bin/env python

"""
lightning gvcf2numpy -tag-library ~/keep/by_id/su92l-4zz18-92bx4zjg5hgs3yc/tagset.fa.gz -ref ./hg38.fa.gz ~/keep/by_id/su92l-4zz18-s3e6as6uzsoocsb > example.npy
example-1k-pca.py example.npy
example-1k-plot.py example.npy.pca.npy sample_info.csv ~/keep/by_id/su92l-4zz18-s3e6as6uzsoocsb
ls -l example.npy.pca.npy.png
"""

import sys
infile = sys.argv[1]

import scipy
X = scipy.load(infile)

from sklearn.decomposition import PCA
pca = PCA(n_components=4)
X = pca.fit_transform(X)
scipy.save(infile+".pca.npy", X)
