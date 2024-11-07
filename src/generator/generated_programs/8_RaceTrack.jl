using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("race_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("race_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[0, "track id"], Any[1, "track id"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[0, "track id"], Any[1, "track id"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model RaceTrackModel begin
    @class Race begin
        race_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        class ~ ChooseUniformly(possibilities[:class])
        date ~ ChooseUniformly(possibilities[:date])
        track_id ~ ChooseUniformly(possibilities[:track_id])
    end

    @class Track begin
        track_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        seating ~ ChooseUniformly(possibilities[:seating])
        year_opened ~ ChooseUniformly(possibilities[:year_opened])
    end

    @class Obs begin
        race ~ Race
        track ~ Track
    end
end

query = @query RaceTrackModel.Obs [
    race_id race.race_id
    race_name race.name
    race_class race.class
    race_date race.date
    track_id track.track_id
    track_name track.name
    track_location track.location
    track_seating track.seating
    track_year_opened track.year_opened
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
