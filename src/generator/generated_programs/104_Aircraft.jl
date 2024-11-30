using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("pilot_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("pilot_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "pilot id"], Any[0, "name"], Any[0, "age"], Any[1, "aircraft id"], Any[1, "aircraft"], Any[1, "description"], Any[1, "max gross weight"], Any[1, "total disk area"], Any[1, "max disk loading"], Any[2, "round"], Any[2, "location"], Any[2, "country"], Any[2, "date"], Any[2, "fastest qualifying"], Any[2, "winning pilot"], Any[2, "winning aircraft"], Any[3, "airport id"], Any[3, "airport name"], Any[3, "total passengers"], Any[3, "% change 2007"], Any[3, "international passengers"], Any[3, "domestic passengers"], Any[3, "transit passengers"], Any[3, "aircraft movements"], Any[3, "freight metric tonnes"], Any[4, "id"], Any[4, "airport id"], Any[4, "aircraft id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "pilot id"], Any[0, "name"], Any[0, "age"], Any[1, "aircraft id"], Any[1, "aircraft"], Any[1, "description"], Any[1, "max gross weight"], Any[1, "total disk area"], Any[1, "max disk loading"], Any[2, "round"], Any[2, "location"], Any[2, "country"], Any[2, "date"], Any[2, "fastest qualifying"], Any[2, "winning pilot"], Any[2, "winning aircraft"], Any[3, "airport id"], Any[3, "airport name"], Any[3, "total passengers"], Any[3, "% change 2007"], Any[3, "international passengers"], Any[3, "domestic passengers"], Any[3, "transit passengers"], Any[3, "aircraft movements"], Any[3, "freight metric tonnes"], Any[4, "id"], Any[4, "airport id"], Any[4, "aircraft id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["winning pilot", "winning aircraft", "aircraft id", "airport id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "pilot id"], Any[0, "name"], Any[0, "age"], Any[1, "aircraft"], Any[1, "description"], Any[1, "max gross weight"], Any[1, "total disk area"], Any[1, "max disk loading"], Any[2, "round"], Any[2, "location"], Any[2, "country"], Any[2, "date"], Any[2, "fastest qualifying"], Any[3, "airport name"], Any[3, "total passengers"], Any[3, "% change 2007"], Any[3, "international passengers"], Any[3, "domestic passengers"], Any[3, "transit passengers"], Any[3, "aircraft movements"], Any[3, "freight metric tonnes"], Any[4, "id"]]
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





PClean.@model AircraftModel begin
    @class Pilot begin
        pilot_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Aircraft begin
        aircraft_id ~ Unmodeled()
        aircraft ~ ChooseUniformly(possibilities[:aircraft])
        description ~ ChooseUniformly(possibilities[:description])
        max_gross_weight ~ ChooseUniformly(possibilities[:max_gross_weight])
        total_disk_area ~ ChooseUniformly(possibilities[:total_disk_area])
        max_disk_loading ~ ChooseUniformly(possibilities[:max_disk_loading])
    end

    @class Airport begin
        airport_id ~ Unmodeled()
        airport_name ~ ChooseUniformly(possibilities[:airport_name])
        total_passengers ~ ChooseUniformly(possibilities[:total_passengers])
        %_change_2007 ~ ChooseUniformly(possibilities[:%_change_2007])
        international_passengers ~ ChooseUniformly(possibilities[:international_passengers])
        domestic_passengers ~ ChooseUniformly(possibilities[:domestic_passengers])
        transit_passengers ~ ChooseUniformly(possibilities[:transit_passengers])
        aircraft_movements ~ ChooseUniformly(possibilities[:aircraft_movements])
        freight_metric_tonnes ~ ChooseUniformly(possibilities[:freight_metric_tonnes])
    end

    @class Obs begin
        pilot ~ Pilot
        aircraft ~ Aircraft
        airport ~ Airport
        round ~ ChooseUniformly(possibilities[:round])
        location ~ ChooseUniformly(possibilities[:location])
        country ~ ChooseUniformly(possibilities[:country])
        date ~ ChooseUniformly(possibilities[:date])
        fastest_qualifying ~ ChooseUniformly(possibilities[:fastest_qualifying])
        id ~ Unmodeled()
    end
end

query = @query AircraftModel.Obs [
    pilot_id pilot.pilot_id
    pilot_name pilot.name
    pilot_age pilot.age
    aircraft_id aircraft.aircraft_id
    aircraft aircraft.aircraft
    aircraft_description aircraft.description
    aircraft_max_gross_weight aircraft.max_gross_weight
    aircraft_total_disk_area aircraft.total_disk_area
    aircraft_max_disk_loading aircraft.max_disk_loading
    match_round round
    match_location location
    match_country country
    match_date date
    match_fastest_qualifying fastest_qualifying
    airport_id airport.airport_id
    airport_name airport.airport_name
    airport_total_passengers airport.total_passengers
    airport_%_change_2007 airport.%_change_2007
    airport_international_passengers airport.international_passengers
    airport_domestic_passengers airport.domestic_passengers
    airport_transit_passengers airport.transit_passengers
    airport_aircraft_movements airport.aircraft_movements
    airport_freight_metric_tonnes airport.freight_metric_tonnes
    airport_aircraft_id id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
