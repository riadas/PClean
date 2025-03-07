#!/bin/bash

nsamples=$1
nruns=$2

# home_dir="/afs/csail.mit.edu/u/r/riadas/phd/PClean"
home_dir="/Users/riadas/Documents/phd/classes/databases/final_project/PClean"

ids=( 7 9 10 ) # ( 1 5 10 )

# for i in $(seq 1 $nruns);
for i in ${ids[*]}
do
    echo "repeat $i out of $nruns: nsamples is $1"
    python $home_dir/src/generator/question_asking_experiment.py $nsamples $i # > "${home_dir}/question_asking_experiment/logs/wrapper_logs/run_num_samples_${nsamples}_subset_size_${subsetsize}_idx_$i.txt"
done