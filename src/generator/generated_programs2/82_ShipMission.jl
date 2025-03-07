using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("mission_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("mission_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "ship id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "ship id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "ship id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "ship id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "ship id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[2, 8]])
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







PClean.@model ShipMissionModel begin
    @class Ship begin
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        tonnage ~ ChooseUniformly(possibilities[:tonnage])
    end

    @class Mission begin
        ship ~ Ship
        code ~ ChooseUniformly(possibilities[:code])
        launched_year ~ ChooseUniformly(possibilities[:launched_year])
        location ~ ChooseUniformly(possibilities[:location])
        speed_knots ~ ChooseUniformly(possibilities[:speed_knots])
        fate ~ ChooseUniformly(possibilities[:fate])
    end

    @class Obs begin
        mission ~ Mission
    end
end

query = @query ShipMissionModel.Obs [
    mission_id mission.mission_id
    mission_code mission.code
    mission_launched_year mission.launched_year
    mission_location mission.location
    mission_speed_knots mission.speed_knots
    mission_fate mission.fate
    ship_id mission.ship.ship_id
    ship_name mission.ship.name
    ship_type mission.ship.type
    ship_nationality mission.ship.nationality
    ship_tonnage mission.ship.tonnage
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
