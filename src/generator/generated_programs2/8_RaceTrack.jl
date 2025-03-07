using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("race_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("race_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[0, "track id"], Any[1, "track id"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[0, "track id"], Any[1, "track id"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[0, "track id"], Any[1, "track id"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[0, "track id"], Any[1, "track id"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[0, "track id"], Any[1, "track id"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 6]])
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







PClean.@model RaceTrackModel begin
    @class Track begin
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        seating ~ ChooseUniformly(possibilities[:seating])
        year_opened ~ ChooseUniformly(possibilities[:year_opened])
    end

    @class Race begin
        name ~ ChooseUniformly(possibilities[:name])
        class ~ ChooseUniformly(possibilities[:class])
        date ~ ChooseUniformly(possibilities[:date])
        track ~ Track
    end

    @class Obs begin
        race ~ Race
    end
end

query = @query RaceTrackModel.Obs [
    race_id race.race_id
    race_name race.name
    race_class race.class
    race_date race.date
    track_id race.track.track_id
    track_name race.track.name
    track_location race.track.location
    track_seating race.track.seating
    track_year_opened race.track.year_opened
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
