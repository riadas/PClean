home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
# subset_size = 100
num_repeats = 3
text = ""
for subset_size in [200, 300] # , 200, 300
    for method in ["synthesis_from_jsons"] # ["synthesis_from_jsons", "direct_synthesis"]
        for benchmark in ["hospital"] # "rents", "hospital", 
            results = []
            for filename in readdir("$(home_directory)/multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)")
                if occursin("base2", filename)
                    continue
                end
                println("HELLO")
                println(filename)
                if occursin(".jl", filename) 
                    println("read text")
                    open("$(home_directory)/multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/$(filename)") do f 
                        text = read(f, String)
                    end
                    println("done reading text")

                    # modify subset_size in text and write back to file
                    println("replace text")
                    text = replace(text, "subset_size = size(dirty_table, 1)" => "subset_size = $(subset_size)")
                    println("write text")
                    open("$(home_directory)/multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/$(filename)", "w") do f 
                        write(f, text)
                    end
                    println("done writing text")
                    # run the program on the data subset 
                    accuracies = []
                    for i in 1:num_repeats
                        println("repeat $(i)") 
                        include("$(home_directory)/multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/$(filename)")
                        push!(accuracies, accuracy)    
                    end
                    push!(results, [filename, sum(map(x -> x.f1, accuracies))/num_repeats, accuracies])
                    # set subset_size back to default and write to file
                    println("replace text 2") 
                    text = replace(text, "subset_size = $(subset_size)" => "subset_size = size(dirty_table, 1)")
                    println("write text 2")
                    open("$(home_directory)/multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/$(filename)", "w") do f 
                        write(f, text)
                    end
                    println("done writing text")
                end
            end
            max_accuracy = maximum(map(tup -> tup[2], results))
            best_program = findall(tup -> tup[2] == max_accuracy, results)[1][1]
            if !isdir("$(home_directory)/multiple_hypotheses_base/results/$(method)")
                mkdir("$(home_directory)/multiple_hypotheses_base/results/$(method)")
            end
            open("$(home_directory)/multiple_hypotheses_base/results/$(method)/$(benchmark)_results_subset_$(subset_size).txt", "w") do f 
                write(f, join(results, "\n"))
            end
            open("$(home_directory)/multiple_hypotheses_base/results/$(method)/$(benchmark)_best_program_$(subset_size).jl", "w") do f 
                write(f, string(results[best_program]))
            end
        end
    end
end