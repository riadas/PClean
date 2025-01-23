#!/bin/bash

# home_dir="/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
# home_dir="/afs/csail.mit.edu/u/r/riadas/phd/PClean"
home_directory = "/scratch/riadas/PClean"

methods=("synthesis_from_jsons") # ("synthesis_from_jsons" "direct_synthesis")
benchmarks=("rents")
counter=0
for method in ${methods[@]}
do 
    for benchmark in ${benchmarks[@]}; 
    do
        for filename in "$home_dir/multiple_hypotheses_base2/hypotheses_copy/$method/$benchmark"/*
        do
            echo new_run
            echo $method
            echo $benchmark
            echo $filename
            echo $counter 
            ((counter++))
            julia --project=$home_dir $home_dir/multiple_hypotheses_base2/scripts/compute_best_overall_program_rents.jl $method $benchmark $filename > "${home_dir}/logs/${method}_${benchmark}_${counter}.txt" &
            sleep 10
        done
    done
    # sleep 32400
done
