using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("pilot_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("pilot_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "pilot id"], Any[0, "name"], Any[0, "age"], Any[1, "aircraft id"], Any[1, "aircraft"], Any[1, "description"], Any[1, "max gross weight"], Any[1, "total disk area"], Any[1, "max disk loading"], Any[2, "round"], Any[2, "location"], Any[2, "country"], Any[2, "date"], Any[2, "fastest qualifying"], Any[2, "winning pilot"], Any[2, "winning aircraft"], Any[3, "airport id"], Any[3, "airport name"], Any[3, "total passengers"], Any[3, "% change 2007"], Any[3, "international passengers"], Any[3, "domestic passengers"], Any[3, "transit passengers"], Any[3, "aircraft movements"], Any[3, "freight metric tonnes"], Any[4, "id"], Any[4, "airport id"], Any[4, "aircraft id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[15, 1], Any[16, 4], Any[28, 4], Any[27, 17]])
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







PClean.@model AircraftModel begin
    @class Pilot begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Aircraft begin
        aircraft ~ ChooseUniformly(possibilities[:aircraft])
        description ~ ChooseUniformly(possibilities[:description])
        max_gross_weight ~ ChooseUniformly(possibilities[:max_gross_weight])
        total_disk_area ~ ChooseUniformly(possibilities[:total_disk_area])
        max_disk_loading ~ ChooseUniformly(possibilities[:max_disk_loading])
    end

    @class Match begin
        round ~ ChooseUniformly(possibilities[:round])
        location ~ ChooseUniformly(possibilities[:location])
        country ~ ChooseUniformly(possibilities[:country])
        date ~ ChooseUniformly(possibilities[:date])
        fastest_qualifying ~ ChooseUniformly(possibilities[:fastest_qualifying])
        pilot ~ Pilot
        aircraft ~ Aircraft
    end

    @class Airport begin
        airport_name ~ ChooseUniformly(possibilities[:airport_name])
        total_passengers ~ ChooseUniformly(possibilities[:total_passengers])
        %_change_2007 ~ ChooseUniformly(possibilities[:%_change_2007])
        international_passengers ~ ChooseUniformly(possibilities[:international_passengers])
        domestic_passengers ~ ChooseUniformly(possibilities[:domestic_passengers])
        transit_passengers ~ ChooseUniformly(possibilities[:transit_passengers])
        aircraft_movements ~ ChooseUniformly(possibilities[:aircraft_movements])
        freight_metric_tonnes ~ ChooseUniformly(possibilities[:freight_metric_tonnes])
    end

    @class Airport_aircraft begin
        id ~ Unmodeled()
        aircraft ~ Aircraft
    end

    @class Obs begin
        match ~ Match
        airport_aircraft ~ Airport_aircraft
    end
end

query = @query AircraftModel.Obs [
    pilot_id match.pilot.pilot_id
    pilot_name match.pilot.name
    pilot_age match.pilot.age
    aircraft_id match.aircraft.aircraft_id
    aircraft match.aircraft.aircraft
    aircraft_description match.aircraft.description
    aircraft_max_gross_weight match.aircraft.max_gross_weight
    aircraft_total_disk_area match.aircraft.total_disk_area
    aircraft_max_disk_loading match.aircraft.max_disk_loading
    match_round match.round
    match_location match.location
    match_country match.country
    match_date match.date
    match_fastest_qualifying match.fastest_qualifying
    airport_id airport_aircraft.airport.airport_id
    airport_name airport_aircraft.airport.airport_name
    airport_total_passengers airport_aircraft.airport.total_passengers
    airport_%_change_2007 airport_aircraft.airport.%_change_2007
    airport_international_passengers airport_aircraft.airport.international_passengers
    airport_domestic_passengers airport_aircraft.airport.domestic_passengers
    airport_transit_passengers airport_aircraft.airport.transit_passengers
    airport_aircraft_movements airport_aircraft.airport.aircraft_movements
    airport_freight_metric_tonnes airport_aircraft.airport.freight_metric_tonnes
    airport_aircraft_id airport_aircraft.id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
