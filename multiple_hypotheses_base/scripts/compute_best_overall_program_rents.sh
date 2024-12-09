#!/bin/bash

# home_dir="/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
home_dir="/afs/csail.mit.edu/u/r/riadas/phd/PClean"
methods=("synthesis_from_jsons" "direct_synthesis")
benchmarks=("rents")
for method in ${methods[@]}
do 
    for benchmark in ${benchmarks[@]}; 
    do
        for filename in "$home_dir/multiple_hypotheses_base/hypotheses_copy/$(method)/$(benchmark)"/*
        echo new_run
        echo $method
        echo $benchmark
        echo $filename
        # julia --project=$home_dir $home_dir/multiple_hypotheses_base/scripts/compute_best_overall_program_rents.jl $method $benchmark $filename > "$home_dir/logs/$method_$benchmark_$filename.txt" &
        sleep 10
    done
    # sleep 32400
done