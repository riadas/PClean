using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("aircraft_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("aircraft_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "aircraft id"], Any[0, "order year"], Any[0, "manufacturer"], Any[0, "model"], Any[0, "fleet series"], Any[0, "powertrain"], Any[0, "fuel propulsion"], Any[1, "pilot id"], Any[1, "pilot name"], Any[1, "rank"], Any[1, "age"], Any[1, "nationality"], Any[1, "position"], Any[1, "join year"], Any[1, "team"], Any[2, "record id"], Any[2, "pilot id"], Any[2, "aircraft id"], Any[2, "date"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "aircraft id"], Any[0, "order year"], Any[0, "manufacturer"], Any[0, "model"], Any[0, "fleet series"], Any[0, "powertrain"], Any[0, "fuel propulsion"], Any[1, "pilot id"], Any[1, "pilot name"], Any[1, "rank"], Any[1, "age"], Any[1, "nationality"], Any[1, "position"], Any[1, "join year"], Any[1, "team"], Any[2, "record id"], Any[2, "pilot id"], Any[2, "aircraft id"], Any[2, "date"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["aircraft id", "pilot id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "order year"], Any[0, "manufacturer"], Any[0, "model"], Any[0, "fleet series"], Any[0, "powertrain"], Any[0, "fuel propulsion"], Any[1, "pilot name"], Any[1, "rank"], Any[1, "age"], Any[1, "nationality"], Any[1, "position"], Any[1, "join year"], Any[1, "team"], Any[2, "record id"], Any[2, "date"]]
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





PClean.@model PilotRecordModel begin
    @class Aircraft begin
        aircraft_id ~ Unmodeled()
        order_year ~ ChooseUniformly(possibilities[:order_year])
        manufacturer ~ ChooseUniformly(possibilities[:manufacturer])
        model ~ ChooseUniformly(possibilities[:model])
        fleet_series ~ ChooseUniformly(possibilities[:fleet_series])
        powertrain ~ ChooseUniformly(possibilities[:powertrain])
        fuel_propulsion ~ ChooseUniformly(possibilities[:fuel_propulsion])
    end

    @class Pilot begin
        pilot_id ~ Unmodeled()
        pilot_name ~ ChooseUniformly(possibilities[:pilot_name])
        rank ~ ChooseUniformly(possibilities[:rank])
        age ~ ChooseUniformly(possibilities[:age])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        position ~ ChooseUniformly(possibilities[:position])
        join_year ~ ChooseUniformly(possibilities[:join_year])
        team ~ ChooseUniformly(possibilities[:team])
    end

    @class Obs begin
        aircraft ~ Aircraft
        pilot ~ Pilot
        record_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
    end
end

query = @query PilotRecordModel.Obs [
    aircraft_id aircraft.aircraft_id
    aircraft_order_year aircraft.order_year
    aircraft_manufacturer aircraft.manufacturer
    aircraft_model aircraft.model
    aircraft_fleet_series aircraft.fleet_series
    aircraft_powertrain aircraft.powertrain
    aircraft_fuel_propulsion aircraft.fuel_propulsion
    pilot_id pilot.pilot_id
    pilot_name pilot.pilot_name
    pilot_rank pilot.rank
    pilot_age pilot.age
    pilot_nationality pilot.nationality
    pilot_position pilot.position
    pilot_join_year pilot.join_year
    pilot_team pilot.team
    pilot_record_record_id record_id
    pilot_record_date date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
