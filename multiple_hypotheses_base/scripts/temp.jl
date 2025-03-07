# using CSV
# home_directory = "/afs/csail.mit.edu/u/r/riadas/phd/PClean"

# include("$(home_directory)/src/generator/evaluate_program.jl")


# dirty_data = CSV.File("$(home_directory)/datasets/rents_dirty.csv") |> DataFrame
# clean_data = CSV.File("$(home_directory)/datasets/rents_clean.csv") |> DataFrame

# results = []
# for filename in readdir("$(home_directory)/multiple_hypotheses_base/results/datasets/synthesis_from_jsons/rents")
#     # evaluate accuracy
#     out_filename = "$(home_directory)/multiple_hypotheses_base/results/datasets/synthesis_from_jsons/rents/$(filename)"
#     accuracy = evaluate_accuracy_external(dirty_data, clean_data, out_filename)
#     push!(results, [out_filename, accuracy])
# end
# max_accuracy = maximum(map(tup -> tup[2].f1, results))
# best_program_index = findall(tup -> tup[2].f1 == max_accuracy, results)

# open("$(home_directory)/multiple_hypotheses_base/results/synthesis_from_jsons/rents_best_program_overall.jl", "w") do f 
#     write(f, string(results[best_program_index]))
# end

# open("$(home_directory)/multiple_hypotheses_base/results/synthesis_from_jsons/rents_results_overall.txt", "w") do f 
#     write(f, join(results, "\n"))
# end

# home_directory = "/afs/csail.mit.edu/u/r/riadas/phd/PClean"
home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"

text = ""
for folder in ["hypotheses", "hypotheses_copy"]
    for method in ["synthesis_from_jsons", "direct_synthesis"]
        for benchmark in ["rents", "hospital", "flights"]
            dir = "$(home_directory)/multiple_hypotheses_base/$(folder)/$(method)/$(benchmark)"
            new_dir = "$(home_directory)/multiple_hypotheses_base2/$(folder)/$(method)/$(benchmark)"
            for filename in readdir(dir)
                if occursin("format2", filename) 
                    open("$(dir)/$(filename)") do f
                        global text = read(f, String)       
                    end
                
                    if !occursin("accuracy = ", text)
                        println("$(dir)/$(filename)")
                        old_line = "println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))"
                        new_line = "accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)\nprintln(accuracy)"
        
                        new_text = replace(text, old_line => new_line)
                        open("$(new_dir)/$(filename)", "w") do f
                            println(dir)
                            println(new_dir)
                            # println(new_text)
                            write(f, new_text)       
                        end
                    end
                
                end
                
            end
        end
    end
end
