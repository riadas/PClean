#!/bin/bash

# home_dir="/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
home_dir="/afs/csail.mit.edu/u/r/riadas/phd/PClean"
methods=("synthesis_from_jsons" "direct_synthesis")
benchmarks=("flights" "hospital") 
for method in ${methods[@]}
do 
    for benchmark in ${benchmarks[@]}; 
    do
        echo new_run
        echo $method
        echo $benchmark
        julia --project=$home_dir $home_dir/multiple_hypotheses_base/scripts/compute_best_overall_program.jl $method $benchmark > $home_dir/logs/$method_$benchmark.txt &
        sleep 10
    done
done