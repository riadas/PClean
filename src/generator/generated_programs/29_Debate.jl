using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("people_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("people_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["negative", "affirmative", "debate id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "if affirmative win"]]
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





PClean.@model DebateModel begin
    @class People begin
        people_id ~ Unmodeled()
        district ~ ChooseUniformly(possibilities[:district])
        name ~ ChooseUniformly(possibilities[:name])
        party ~ ChooseUniformly(possibilities[:party])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Debate begin
        debate_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        num_of_audience ~ ChooseUniformly(possibilities[:num_of_audience])
    end

    @class Obs begin
        people ~ People
        debate ~ Debate
        if_affirmative_win ~ ChooseUniformly(possibilities[:if_affirmative_win])
    end
end

query = @query DebateModel.Obs [
    people_id people.people_id
    people_district people.district
    people_name people.name
    people_party people.party
    people_age people.age
    debate_id debate.debate_id
    debate_date debate.date
    debate_venue debate.venue
    debate_num_of_audience debate.num_of_audience
    debate_people_if_affirmative_win if_affirmative_win
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
