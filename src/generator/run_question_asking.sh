#!/bin/bash

nruns=10 #$1
samples=( 10 ) # ( 1 5 10 )
# subsetsizes=( 0 100 200 300 )

# home_dir="/afs/csail.mit.edu/u/r/riadas/phd/PClean"
home_dir="/Users/riadas/Documents/phd/classes/databases/final_project/PClean"

for nsamples in ${samples[*]}
do
    bash $home_dir/src/generator/question_asking_wrapper.sh $nsamples $nruns
done