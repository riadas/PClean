using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("airlines_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("airlines_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"], Any[2, "source airport"], Any[2, "destination airport"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"], Any[2, "source airport"], Any[2, "destination airport"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Flight2Model begin
    @class Airlines begin
        airline_id ~ Unmodeled()
        airline_name ~ ChooseUniformly(possibilities[:airline_name])
        abbreviation ~ ChooseUniformly(possibilities[:abbreviation])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Airports begin
        city ~ ChooseUniformly(possibilities[:city])
        airport_code ~ ChooseUniformly(possibilities[:airport_code])
        airport_name ~ ChooseUniformly(possibilities[:airport_name])
        country ~ ChooseUniformly(possibilities[:country])
        country_abbrev ~ ChooseUniformly(possibilities[:country_abbrev])
    end

    @class Flights begin
        airline ~ ChooseUniformly(possibilities[:airline])
        flight_number ~ ChooseUniformly(possibilities[:flight_number])
        source_airport ~ ChooseUniformly(possibilities[:source_airport])
        destination_airport ~ ChooseUniformly(possibilities[:destination_airport])
    end

    @class Obs begin
        airlines ~ Airlines
        airports ~ Airports
        flights ~ Flights
    end
end

query = @query Flight2Model.Obs [
    airlines_airline_id airlines.airline_id
    airlines_airline_name airlines.airline_name
    airlines_abbreviation airlines.abbreviation
    airlines_country airlines.country
    airports_city airports.city
    airports_airport_code airports.airport_code
    airports_airport_name airports.airport_name
    airports_country airports.country
    airports_country_abbrev airports.country_abbrev
    flights_airline flights.airline
    flights_flight_number flights.flight_number
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
