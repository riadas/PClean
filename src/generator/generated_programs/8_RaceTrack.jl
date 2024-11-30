using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("race_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("race_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["track id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "race id"], Any[0, "name"], Any[0, "class"], Any[0, "date"], Any[1, "name"], Any[1, "location"], Any[1, "seating"], Any[1, "year opened"]]
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





PClean.@model RaceTrackModel begin
    @class Track begin
        track_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        seating ~ ChooseUniformly(possibilities[:seating])
        year_opened ~ ChooseUniformly(possibilities[:year_opened])
    end

    @class Obs begin
        track ~ Track
        race_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        class ~ ChooseUniformly(possibilities[:class])
        date ~ ChooseUniformly(possibilities[:date])
    end
end

query = @query RaceTrackModel.Obs [
    race_id race_id
    race_name name
    race_class class
    race_date date
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

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
