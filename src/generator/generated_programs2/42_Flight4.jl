using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("routes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("routes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "route id"], Any[0, "destination airport id"], Any[0, "destination airport"], Any[0, "source airport id"], Any[0, "source airport"], Any[0, "airline id"], Any[0, "airline"], Any[0, "code share"], Any[1, "airport id"], Any[1, "name"], Any[1, "city"], Any[1, "country"], Any[1, "x"], Any[1, "y"], Any[1, "elevation"], Any[1, "iata"], Any[1, "icao"], Any[2, "airline id"], Any[2, "name"], Any[2, "iata"], Any[2, "icao"], Any[2, "call sign"], Any[2, "country"], Any[2, "active"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "route id"], Any[0, "destination airport id"], Any[0, "destination airport"], Any[0, "source airport id"], Any[0, "source airport"], Any[0, "airline id"], Any[0, "airline"], Any[0, "code share"], Any[1, "airport id"], Any[1, "name"], Any[1, "city"], Any[1, "country"], Any[1, "x"], Any[1, "y"], Any[1, "elevation"], Any[1, "iata"], Any[1, "icao"], Any[2, "airline id"], Any[2, "name"], Any[2, "iata"], Any[2, "icao"], Any[2, "call sign"], Any[2, "country"], Any[2, "active"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["airline id", "source airport id", "destination airport id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "route id"], Any[0, "destination airport"], Any[0, "source airport"], Any[0, "airline"], Any[0, "code share"], Any[1, "airport id"], Any[1, "name"], Any[1, "city"], Any[1, "country"], Any[1, "x"], Any[1, "y"], Any[1, "elevation"], Any[1, "iata"], Any[1, "icao"], Any[2, "name"], Any[2, "iata"], Any[2, "icao"], Any[2, "call sign"], Any[2, "country"], Any[2, "active"]]
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





PClean.@model Flight4Model begin
    @class Airports begin
        airport_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        city ~ ChooseUniformly(possibilities[:city])
        country ~ ChooseUniformly(possibilities[:country])
        x ~ ChooseUniformly(possibilities[:x])
        y ~ ChooseUniformly(possibilities[:y])
        elevation ~ ChooseUniformly(possibilities[:elevation])
        iata ~ ChooseUniformly(possibilities[:iata])
        icao ~ ChooseUniformly(possibilities[:icao])
    end

    @class Airlines begin
        airline_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        iata ~ ChooseUniformly(possibilities[:iata])
        icao ~ ChooseUniformly(possibilities[:icao])
        call_sign ~ ChooseUniformly(possibilities[:call_sign])
        country ~ ChooseUniformly(possibilities[:country])
        active ~ ChooseUniformly(possibilities[:active])
    end

    @class Routes begin
        route_id ~ Unmodeled()
        airports ~ Airports
        destination_airport ~ ChooseUniformly(possibilities[:destination_airport])
        airports ~ Airports
        source_airport ~ ChooseUniformly(possibilities[:source_airport])
        airlines ~ Airlines
        airline ~ ChooseUniformly(possibilities[:airline])
        code_share ~ ChooseUniformly(possibilities[:code_share])
    end

    @class Obs begin
        routes ~ Routes
    end
end

query = @query Flight4Model.Obs [
    routes_route_id routes.route_id
    routes_destination_airport routes.destination_airport
    routes_source_airport routes.source_airport
    routes_airline routes.airline
    routes_code_share routes.code_share
    airports_airport_id routes.airports.airport_id
    airports_name routes.airports.name
    airports_city routes.airports.city
    airports_country routes.airports.country
    airports_x routes.airports.x
    airports_y routes.airports.y
    airports_elevation routes.airports.elevation
    airports_iata routes.airports.iata
    airports_icao routes.airports.icao
    airlines_airline_id routes.airlines.airline_id
    airlines_name routes.airlines.name
    airlines_iata routes.airlines.iata
    airlines_icao routes.airlines.icao
    airlines_call_sign routes.airlines.call_sign
    airlines_country routes.airlines.country
    airlines_active routes.airlines.active
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
