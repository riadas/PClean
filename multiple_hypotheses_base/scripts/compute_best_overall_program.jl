home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
text = ""
num_repeats = 3

curr_method = ARGS[1]
curr_benchmark = ARGS[2]

for method in [curr_method] #["synthesis_from_jsons"] # ["synthesis_from_jsons", "direct_synthesis"]
    for benchmark in [curr_benchmark] # ["flights", "hospital", "rents"]
        println("compute_overall_best_program: $(benchmark)")
        if method == "direct_synthesis" && benchmark == "flights"
            println("never mind!")
            continue
        end

        # compute f1 scores of each variant and find the max
        # save the results to a file
        results = []
        for filename in readdir("$(home_directory)/multiple_hypotheses_base/hypotheses_copy/$(method)/$(benchmark)")
            println("compute_overall_best_program: $(benchmark), $(filename)")
            text = ""
            open("$(home_directory)/multiple_hypotheses_base/hypotheses_copy/$(method)/$(benchmark)/$(filename)") do f 
                text = read(f, String)
            end
            # compute average accuracy over num_repeats runs
            accuracies = []
            for i in 1:num_repeats
                # modify last line with save function in text and write back to file
                save_file_lines = [
                    """include("$(home_directory)/src/generator/evaluate_program.jl")""",
                    """table = get_table_from_trace(tr.tables[:Obs], query, clean_table, dirty_table)""",
                    """CSV.write("$(home_directory)/multiple_hypotheses_base/results/datasets/$(method)/$(benchmark)/$(replace(filename, ".jl" => "_$(i).csv"))", table)"""
                    ]
                save_file_line = join(save_file_lines, "\n")
                old_program_end = "println(accuracy)"
                new_program_end = "$(old_program_end)\n$(save_file_line)"
                text = replace(text, old_program_end => new_program_end)
                open("$(home_directory)/multiple_hypotheses_base/hypotheses_copy/$(method)/$(benchmark)/$(filename)", "w") do f 
                    write(f, text)
                end
                
                include("$(home_directory)/multiple_hypotheses_base/hypotheses_copy/$(method)/$(benchmark)/$(filename)")
                push!(accuracies, accuracy)

                # remove save file line in text and write to file 
                text = replace(text, new_program_end => old_program_end)
                open("$(home_directory)/multiple_hypotheses_base/hypotheses_copy/$(method)/$(benchmark)/$(filename)", "w") do f 
                    write(f, text)
                end
            end
            push!(results, [filename, sum(map(x -> x.f1, accuracies))/num_repeats, accuracies])
        end
        max_accuracy = maximum(map(tup -> tup[2], results))
        best_program = findall(tup -> tup[2] == max_accuracy, results)[1][1]
        if !isdir("$(home_directory)/multiple_hypotheses_base/results/$(method)")
            mkdir("$(home_directory)/multiple_hypotheses_base/results/$(method)")
        end
        open("$(home_directory)/multiple_hypotheses_base/results/$(method)/$(benchmark)_results_overall.txt", "w") do f 
            write(f, join(results, "\n"))
        end
        open("$(home_directory)/multiple_hypotheses_base/results/$(method)/$(benchmark)_best_program_overall.jl", "w") do f 
            write(f, string(results[best_program]))
        end
    end
end