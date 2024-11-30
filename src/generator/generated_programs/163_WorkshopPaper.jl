using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("workshop_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("workshop_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "workshop id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[1, "submission id"], Any[1, "scores"], Any[1, "author"], Any[1, "college"], Any[2, "submission id"], Any[2, "workshop id"], Any[2, "result"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "workshop id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[1, "submission id"], Any[1, "scores"], Any[1, "author"], Any[1, "college"], Any[2, "submission id"], Any[2, "workshop id"], Any[2, "result"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["workshop id", "submission id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[1, "scores"], Any[1, "author"], Any[1, "college"], Any[2, "result"]]
if length(omitted) == 0 
    column_renaming_dict = Dict(zip(dirty_columns, map(t -> t[2], column_names_without_foreign_keys)))
    column_renaming_dict_reverse = Dict(zip(map(t -> t[2], column_names_without_foreign_keys), dirty_columns))
else
    column_renaming_dict = Dict(zip(sort(dirty_columns), sort(map(t -> t[2], column_names_without_foreign_keys))))
    column_renaming_dict_reverse = Dict(zip(sort(map(t -> t[2], column_names_without_foreign_keys)), sort(dirty_columns)))    
end

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in dirty_columns
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model WorkshopPaperModel begin
    @class Workshop begin
        workshop_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Submission begin
        submission_id ~ Unmodeled()
        scores ~ ChooseUniformly(possibilities[:scores])
        author ~ ChooseUniformly(possibilities[:author])
        college ~ ChooseUniformly(possibilities[:college])
    end

    @class Obs begin
        workshop ~ Workshop
        submission ~ Submission
        result ~ ChooseUniformly(possibilities[:result])
    end
end

query = @query WorkshopPaperModel.Obs [
    workshop_id workshop.workshop_id
    workshop_date workshop.date
    workshop_venue workshop.venue
    workshop_name workshop.name
    submission_id submission.submission_id
    submission_scores submission.scores
    submission_author submission.author
    submission_college submission.college
    acceptance_result result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
