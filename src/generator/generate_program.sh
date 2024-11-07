#!/bin/bash
echo $1
python src/generator/generate_program_claude_$1.py
julia src/generator/generate_program.jl $1
julia --project output_$1.jl