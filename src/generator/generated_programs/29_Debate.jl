using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("people_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("people_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Debate_People begin
        debate_id ~ Unmodeled()
        affirmative ~ ChooseUniformly(possibilities[:affirmative])
        negative ~ ChooseUniformly(possibilities[:negative])
        if_affirmative_win ~ ChooseUniformly(possibilities[:if_affirmative_win])
    end

    @class Obs begin
        people ~ People
        debate ~ Debate
        debate_People ~ Debate_People
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
    debate_people_if_affirmative_win debate_People.if_affirmative_win
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
