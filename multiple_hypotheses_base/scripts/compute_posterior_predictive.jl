using CSV
using DataFrames: DataFrame
using StatsBase: mode

home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
include("$(home_directory)/src/generator/evaluate_program.jl")

for method in ["synthesis_from_jsons"] # ["synthesis_from_jsons", "direct_synthesis"]
    for benchmark in ["flights", "hospital", "rents"]
        # row by row, cell by cell, find the most common value across all the datasets
        csv_names = readdir("$(home_directory)/multiple_hypotheses_base/results/datasets/$(method)/$(benchmark)")
        new_rows = []
        clean_table = CSV.File(replace("datasets/$(benchmark)_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame    
        columns = names(clean_table)

        num_rows = size(clean_table, 1)
        num_cols = size(clean_table, 2)
        for row_index in 1:num_rows
            possible_values = []
            for csv in csv_names
                table = CSV.File(csv) |> DataFrame
                for col_index in 1:num_cols
                    col_name = columns[col_index]
                    if len(possible_values) >= col_index 
                        push!(possible_values[col_index], table[row_index, col_name])
                    else 
                        possible_col_values = []
                        push!(possible_col_values, table[row_index, col_name])
                        push!(possible_values, possible_col_values)
                    end
                end
            end
            row = map(arr -> StatsBase.mode(arr), possible_values)
            push!(new_rows, row)
        end
    end
    matrix = mapreduce(permutedims, vcat, new_rows)
    df = DataFrame(matrix, columns)
    if !isdir("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets")
        mkdir("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets")
    end

    if !isdir("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets/$(method)")
        mkdir("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets/$(method)")
    end

    if !isdir("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets/$(method)/$(benchmark)")
        mkdir("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets/$(method)/$(benchmark)")
    end

    CSV.write("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets/$(method)/$(benchmark).csv", df) 

    # evaluate accuracy
    dirty_table = CSV.File("datasets/$(benchmark)_dirty.csv") |> DataFrame
    out_filename = "$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets/$(method)/$(benchmark).csv"
    accuracy = evaluate_accuracy_external(dirty_data, clean_data, out_filename)
    with open("$(home_directory)/multiple_hypotheses_base/results/posterior_predictive_datasets/$(method)/$(benchmark)_accuracy.txt", "w") do f
        write(f, string(accuracy))
    end
end