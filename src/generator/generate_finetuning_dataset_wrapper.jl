include("generate_finetuning_dataset.jl")

mode = ARGS[1]
num_subsets = parse(Int64, ARGS[2])

_ = generate_prompts_and_completions(mode, num_subsets)