include("generate_program.jl")

test_name = ARGS[1]
benchmark_name = ARGS[2]

custom_schema_file = "question_asking_experiment/inputs/test1_$(test_name).txt"
custom_error_file = "question_asking_experiment/inputs/test2_$(test_name).txt"
custom_data_file = "datasets/$(benchmark_name)_dirty.csv"
custom = [custom_schema_file, custom_error_file, custom_data_file]

program = generate_program(custom=custom)
println(program)

open("question_asking_experiment/outputs/$(test_name)/output.jl", "w+") do file 
    write(file, program)
end