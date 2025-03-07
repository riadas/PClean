#!/bin/bash

julia --project=. src/generator/generate_finetuning_dataset_wrapper.jl conversational_long 0
julia --project=. src/generator/generate_finetuning_dataset_wrapper.jl conversational_long 1
julia --project=. src/generator/generate_finetuning_dataset_wrapper.jl conversational_long 2