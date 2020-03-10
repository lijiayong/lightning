#!/bin/bash

set -ex

PATH="${GOPATH:-${HOME}/go}/bin:${PATH}"
go install
lightning build-docker-image
arv keep docker lightning-runtime

project=su92l-j7d0g-jzei0m9yvgauhjf
gvcf=su92l-4zz18-bgyq36m6gctk63q
info=su92l-4zz18-ykpcoea5nisz74f
fasta=su92l-4zz18-s3e6as6uzsoocsb
tags=su92l-4zz18-92bx4zjg5hgs3yc

unfiltered=$(
    lightning import       -project ${project} \
       -tag-library ${tags}/tagset.fa.gz \
       ${fasta})
filtered=$(
    lightning filter       -project ${project} \
       -i ${unfiltered} \
       -min-coverage 0.9 -max-variants 30)
numpy=$(
    lightning export-numpy -project ${project} \
       -i ${filtered})
pca=$(
    lightning pca          -project ${project} \
       -i ${numpy})
plot=$(
    lightning plot         -project ${project} \
       -i ${pca} \
       -labels-csv ${info}/sample_info.csv \
       -sample-fasta-dir ${fasta})
echo >&2 "https://workbench2.${plot%%-*}.arvadosapi.com/collections/${plot}"
echo ${plot%%/*}
