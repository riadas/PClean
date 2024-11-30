using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("routes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("routes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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

    @class Obs begin
        airports ~ Airports
        airlines ~ Airlines
        route_id ~ Unmodeled()
        destination_airport ~ ChooseUniformly(possibilities[:destination_airport])
        source_airport ~ ChooseUniformly(possibilities[:source_airport])
        airline ~ ChooseUniformly(possibilities[:airline])
        code_share ~ ChooseUniformly(possibilities[:code_share])
    end
end

query = @query Flight4Model.Obs [
    routes_route_id route_id
    routes_destination_airport destination_airport
    routes_source_airport source_airport
    routes_airline airline
    routes_code_share code_share
    airports_airport_id airports.airport_id
    airports_name airports.name
    airports_city airports.city
    airports_country airports.country
    airports_x airports.x
    airports_y airports.y
    airports_elevation airports.elevation
    airports_iata airports.iata
    airports_icao airports.icao
    airlines_airline_id airlines.airline_id
    airlines_name airlines.name
    airlines_iata airlines.iata
    airlines_icao airlines.icao
    airlines_call_sign airlines.call_sign
    airlines_country airlines.country
    airlines_active airlines.active
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
