#!/bin/bash

go run . build-docker-image
arv keep docker lightning-runtime

project=su92l-j7d0g-jzei0m9yvgauhjf
gvcf=su92l-4zz18-ykpcoea5nisz74f
fasta=su92l-4zz18-s3e6as6uzsoocsb
tags=su92l-4zz18-92bx4zjg5hgs3yc

go run . import       -project ${project} \
   -tag-library ~/keep/by_id/${tags}/tagset.fa.gz \
   ~/keep/by_id/${fasta}
go run . filter       -project ${project} \
   -i ~/keep/by_id/su92l-4zz18-fcyucnod8y4515p/library.gob \
   -min-coverage 0.9 -max-variants 30
go run . export-numpy -project ${project} \
   -i ~/keep/by_id/su92l-4zz18-l40xcd2l6dmphaj/library.gob
go run . pca          -project ${project} \
   -i ~/keep/by_id/su92l-4zz18-i6fzfoxpdh38yk4/library.npy
go run . plot         -project ${project} \
   -i ~/keep/by_id/su92l-4zz18-zqfo7qc3tadh6zb/pca.npy \
   -labels-csv ~/keep/by_id/${gvcf}/sample_info.csv \
   -sample-fasta-dir ~/keep/by_id/${fasta}
