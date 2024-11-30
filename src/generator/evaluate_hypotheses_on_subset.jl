subset_size = 100

results = []
for benchmark in ["flights", "hospital", "rents"]
    for filename in readdir("multiple_hypotheses/$(benchmark)")
        if ".jl" in filename 
            open("multiple_hypotheses/$(benchmark)/$(filename)") do f 
                global text = read(f, String)
            end
            # modify subset_size in text and write back to file
            text = replace(text, "subset_size = length(dirty_table)" => "subset_size = $(subset_size)")
            open("multiple_hypotheses/$(benchmark)/$(filename)") do f 
                write(f, text)
            end
            # run the program on the data subset 
            include("multiple_hypotheses/$(benchmark)/$(filename)")
            push!(results, [filename, accuracy])
        end
    end
    max_accuracy = maximum(map(tup -> tup[2], results))
    best_program = findall(tup -> tup[2] == max_accuracy, results)[1][1]
    if !isdir("multiple_hypotheses/results")
        mkdir("multiple_hypotheses/results")
    end
    open("multiple_hypotheses/results/$(benchmark)_results.txt") do f 
        write(f, join(results, "\n"))
    end
    open("multiple_hypotheses/results/$(benchmark)_best_program.jl") do f 
        write(f, best_program)
    end
end