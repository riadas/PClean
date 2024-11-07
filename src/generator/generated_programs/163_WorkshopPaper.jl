using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("workshop_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("workshop_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "workshop id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[1, "submission id"], Any[1, "scores"], Any[1, "author"], Any[1, "college"], Any[2, "submission id"], Any[2, "workshop id"], Any[2, "result"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "workshop id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[1, "submission id"], Any[1, "scores"], Any[1, "author"], Any[1, "college"], Any[2, "submission id"], Any[2, "workshop id"], Any[2, "result"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Acceptance begin
        submission_id ~ Unmodeled()
        workshop_id ~ ChooseUniformly(possibilities[:workshop_id])
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Obs begin
        workshop ~ Workshop
        submission ~ Submission
        acceptance ~ Acceptance
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
    acceptance_result acceptance.result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
