home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
# subset_size = 100
num_repeats = 3
text = ""
accuracy = 0
for subset_size in [100] # , 200, 300
    for method in ["direct_synthesis"] # ["synthesis_from_jsons", "direct_synthesis"]
        for benchmark in ["rents"] # "rents", "hospital", 
            results = []
            files = readdir("$(home_directory)/multiple_hypotheses_base/results/$(method)")
            filename = "$(benchmark)_best_program_$(subset_size).jl"
            println("SUBSET_SIZE: $(subset_size), FILENAME: $(filename)")
            if !(filename in files)
                println("missing best_program file!")
                continue
            end
            println("exists!")
            text = ""
            open("$(home_directory)/multiple_hypotheses_base/results/$(method)/$(filename)") do f
                text = read(f, String)
            end

            subset_result = eval(Meta.parse(text))
            best_program_filename = subset_result[1]
            accuracies = []
            for i in 1:num_repeats 
                include("$(home_directory)/multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/$(best_program_filename)")
                push!(accuracies, accuracy.f1)
            end
            println("ACCURACIES")
            println(accuracies)

            avg_accuracy = sum(accuracies)/num_repeats

            if !isdir("$(home_directory)/multiple_hypotheses_base/results/$(method)")
                mkdir("$(home_directory)/multiple_hypotheses_base/results/$(method)")
            end
            open("$(home_directory)/multiple_hypotheses_base/results/$(method)/$(benchmark)_results_subset_$(subset_size)_ACCURACY.txt", "w") do f 
                write(f, string(avg_accuracy))
            end

        end
    end
end