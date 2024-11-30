using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("mission_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("mission_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["ship id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]]
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





PClean.@model ShipMissionModel begin
    @class Ship begin
        ship_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        tonnage ~ ChooseUniformly(possibilities[:tonnage])
    end

    @class Obs begin
        ship ~ Ship
        mission_id ~ Unmodeled()
        code ~ ChooseUniformly(possibilities[:code])
        launched_year ~ ChooseUniformly(possibilities[:launched_year])
        location ~ ChooseUniformly(possibilities[:location])
        speed_knots ~ ChooseUniformly(possibilities[:speed_knots])
        fate ~ ChooseUniformly(possibilities[:fate])
    end
end

query = @query ShipMissionModel.Obs [
    mission_id mission_id
    mission_code code
    mission_launched_year launched_year
    mission_location location
    mission_speed_knots speed_knots
    mission_fate fate
    ship_id ship.ship_id
    ship_name ship.name
    ship_type ship.type
    ship_nationality ship.nationality
    ship_tonnage ship.tonnage
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
