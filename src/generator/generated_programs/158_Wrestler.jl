using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("wrestler_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("wrestler_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "wrestler id"], Any[0, "name"], Any[0, "reign"], Any[0, "days held"], Any[0, "location"], Any[0, "event"], Any[1, "elimination id"], Any[1, "wrestler id"], Any[1, "team"], Any[1, "eliminated by"], Any[1, "elimination move"], Any[1, "time"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "wrestler id"], Any[0, "name"], Any[0, "reign"], Any[0, "days held"], Any[0, "location"], Any[0, "event"], Any[1, "elimination id"], Any[1, "wrestler id"], Any[1, "team"], Any[1, "eliminated by"], Any[1, "elimination move"], Any[1, "time"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model WrestlerModel begin
    @class Wrestler begin
        wrestler_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        reign ~ ChooseUniformly(possibilities[:reign])
        days_held ~ ChooseUniformly(possibilities[:days_held])
        location ~ ChooseUniformly(possibilities[:location])
        event ~ ChooseUniformly(possibilities[:event])
    end

    @class Elimination begin
        elimination_id ~ ChooseUniformly(possibilities[:elimination_id])
        wrestler_id ~ ChooseUniformly(possibilities[:wrestler_id])
        team ~ ChooseUniformly(possibilities[:team])
        eliminated_by ~ ChooseUniformly(possibilities[:eliminated_by])
        elimination_move ~ ChooseUniformly(possibilities[:elimination_move])
        time ~ ChooseUniformly(possibilities[:time])
    end

    @class Obs begin
        wrestler ~ Wrestler
        elimination ~ Elimination
    end
end

query = @query WrestlerModel.Obs [
    wrestler_id wrestler.wrestler_id
    wrestler_name wrestler.name
    wrestler_reign wrestler.reign
    wrestler_days_held wrestler.days_held
    wrestler_location wrestler.location
    wrestler_event wrestler.event
    elimination_id elimination.elimination_id
    elimination_team elimination.team
    elimination_eliminated_by elimination.eliminated_by
    elimination_move elimination.elimination_move
    elimination_time elimination.time
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
