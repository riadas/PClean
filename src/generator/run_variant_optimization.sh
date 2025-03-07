#!/bin/bash

# generate hypothesis variants
julia --project=. multiple_hypotheses_base2/scripts/generate_multiple_hypotheses2.jl

# compute best overall program across variants
bash multiple_hypotheses_base2/scripts/compute_best_overall_program.sh

# evaluate variants on subsets of full data
julia --project=. multiple_hypotheses_base2/scripts/evaluate_hypotheses_on_subset.jl

# evaluate variant on subsets on full datasets
julia --project=. multiple_hypotheses_base2/scripts/evaluate_best_subset_programs_on_full_data.jl