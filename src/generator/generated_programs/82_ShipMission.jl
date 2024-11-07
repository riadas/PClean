using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("mission_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("mission_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "ship id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "mission id"], Any[0, "ship id"], Any[0, "code"], Any[0, "launched year"], Any[0, "location"], Any[0, "speed knots"], Any[0, "fate"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "nationality"], Any[1, "tonnage"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ShipMissionModel begin
    @class Mission begin
        mission_id ~ Unmodeled()
        ship_id ~ ChooseUniformly(possibilities[:ship_id])
        code ~ ChooseUniformly(possibilities[:code])
        launched_year ~ ChooseUniformly(possibilities[:launched_year])
        location ~ ChooseUniformly(possibilities[:location])
        speed_knots ~ ChooseUniformly(possibilities[:speed_knots])
        fate ~ ChooseUniformly(possibilities[:fate])
    end

    @class Ship begin
        ship_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        tonnage ~ ChooseUniformly(possibilities[:tonnage])
    end

    @class Obs begin
        mission ~ Mission
        ship ~ Ship
    end
end

query = @query ShipMissionModel.Obs [
    mission_id mission.mission_id
    mission_code mission.code
    mission_launched_year mission.launched_year
    mission_location mission.location
    mission_speed_knots mission.speed_knots
    mission_fate mission.fate
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

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
