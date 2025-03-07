using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("workshop_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("workshop_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "workshop id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[1, "submission id"], Any[1, "scores"], Any[1, "author"], Any[1, "college"], Any[2, "submission id"], Any[2, "workshop id"], Any[2, "result"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[10, 1], Any[9, 5]])
column_names_without_foreign_keys = filter(tup -> !(tup in foreign_keys), cols)
matching_columns = []
for col in dirty_columns 
    println(col)
    match_indices = findall(tup -> lowercase(join(split(join(split(tup[2], " "), ""), "_"), "")) == lowercase(join(split(join(split(col, " "), ""), "_"), "")), column_names_without_foreign_keys)
    if length(match_indices) > 0
        push!(matching_columns, column_names_without_foreign_keys[match_indices[1]][2])
    else
        error("matching column not found")
    end
end
column_renaming_dict = Dict(zip(dirty_columns, matching_columns))
column_renaming_dict_reverse = Dict(zip(matching_columns, dirty_columns))

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
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Submission begin
        scores ~ ChooseUniformly(possibilities[:scores])
        author ~ ChooseUniformly(possibilities[:author])
        college ~ ChooseUniformly(possibilities[:college])
    end

    @class Acceptance begin
        workshop ~ Workshop
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Obs begin
        acceptance ~ Acceptance
    end
end

query = @query WorkshopPaperModel.Obs [
    workshop_id acceptance.workshop.workshop_id
    workshop_date acceptance.workshop.date
    workshop_venue acceptance.workshop.venue
    workshop_name acceptance.workshop.name
    submission_id acceptance.submission.submission_id
    submission_scores acceptance.submission.scores
    submission_author acceptance.submission.author
    submission_college acceptance.submission.college
    acceptance_result acceptance.result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
