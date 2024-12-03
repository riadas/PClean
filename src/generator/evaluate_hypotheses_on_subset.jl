home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
subset_size = 100
text = ""
for benchmark in ["rents", "hospital", "flights"]
    results = []
    for filename in readdir("$(home_directory)/multiple_hypotheses/$(benchmark)")
        println(filename)
        if occursin(".jl", filename) 
            open("$(home_directory)/multiple_hypotheses/$(benchmark)/$(filename)") do f 
                text = read(f, String)
            end
            # modify subset_size in text and write back to file
            text = replace(text, "subset_size = length(dirty_table)" => "subset_size = $(subset_size)")
            open("$(home_directory)/multiple_hypotheses/$(benchmark)/$(filename)", "w") do f 
                write(f, text)
            end
            # run the program on the data subset 
            include("$(home_directory)/multiple_hypotheses/$(benchmark)/$(filename)")
            push!(results, [filename, accuracy])
        end
    end
    max_accuracy = maximum(map(tup -> tup[2].f1, results))
    best_program = findall(tup -> tup[2].f1 == max_accuracy, results)[1][1]
    if !isdir("$(home_directory)/multiple_hypotheses/results")
        mkdir("$(home_directory)/multiple_hypotheses/results")
    end
    open("$(home_directory)/multiple_hypotheses/results/$(benchmark)_results_subset_$(subset_size).txt", "w") do f 
        write(f, join(results, "\n"))
    end
    open("$(home_directory)/multiple_hypotheses/results/$(benchmark)_best_program_$(subset_size).jl", "w") do f 
        write(f, string(results[best_program]))
    end
end