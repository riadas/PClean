using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("candidate_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("candidate_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "candidate id"], Any[0, "people id"], Any[0, "poll source"], Any[0, "date"], Any[0, "support rate"], Any[0, "consider rate"], Any[0, "oppose rate"], Any[0, "unsure rate"], Any[1, "people id"], Any[1, "sex"], Any[1, "name"], Any[1, "date of birth"], Any[1, "height"], Any[1, "weight"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "candidate id"], Any[0, "people id"], Any[0, "poll source"], Any[0, "date"], Any[0, "support rate"], Any[0, "consider rate"], Any[0, "oppose rate"], Any[0, "unsure rate"], Any[1, "people id"], Any[1, "sex"], Any[1, "name"], Any[1, "date of birth"], Any[1, "height"], Any[1, "weight"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["people id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "candidate id"], Any[0, "poll source"], Any[0, "date"], Any[0, "support rate"], Any[0, "consider rate"], Any[0, "oppose rate"], Any[0, "unsure rate"], Any[1, "sex"], Any[1, "name"], Any[1, "date of birth"], Any[1, "height"], Any[1, "weight"]]
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





PClean.@model CandidatePollModel begin
    @class People begin
        people_id ~ Unmodeled()
        sex ~ ChooseUniformly(possibilities[:sex])
        name ~ ChooseUniformly(possibilities[:name])
        date_of_birth ~ ChooseUniformly(possibilities[:date_of_birth])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
    end

    @class Candidate begin
        candidate_id ~ Unmodeled()
        people ~ People
        poll_source ~ ChooseUniformly(possibilities[:poll_source])
        date ~ ChooseUniformly(possibilities[:date])
        support_rate ~ ChooseUniformly(possibilities[:support_rate])
        consider_rate ~ ChooseUniformly(possibilities[:consider_rate])
        oppose_rate ~ ChooseUniformly(possibilities[:oppose_rate])
        unsure_rate ~ ChooseUniformly(possibilities[:unsure_rate])
    end

    @class Obs begin
        candidate ~ Candidate
    end
end

query = @query CandidatePollModel.Obs [
    candidate_id candidate.candidate_id
    candidate_poll_source candidate.poll_source
    candidate_date candidate.date
    candidate_support_rate candidate.support_rate
    candidate_consider_rate candidate.consider_rate
    candidate_oppose_rate candidate.oppose_rate
    candidate_unsure_rate candidate.unsure_rate
    people_id candidate.people.people_id
    people_sex candidate.people.sex
    people_name candidate.people.name
    people_date_of_birth candidate.people.date_of_birth
    people_height candidate.people.height
    people_weight candidate.people.weight
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
