using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("airlines_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("airlines_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"], Any[2, "source airport"], Any[2, "destination airport"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"], Any[2, "source airport"], Any[2, "destination airport"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"], Any[2, "source airport"], Any[2, "destination airport"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"], Any[2, "source airport"], Any[2, "destination airport"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"], Any[2, "source airport"], Any[2, "destination airport"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 6], Any[12, 6]])
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







PClean.@model Flight2Model begin
    @class Airlines begin
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
        airports ~ Airports
    end

    @class Obs begin
        airlines ~ Airlines
        flights ~ Flights
    end
end

query = @query Flight2Model.Obs [
    airlines_airline_id airlines.airline_id
    airlines_airline_name airlines.airline_name
    airlines_abbreviation airlines.abbreviation
    airlines_country airlines.country
    airports_city flights.airports.city
    airports_airport_code flights.airports.airport_code
    airports_airport_name flights.airports.airport_name
    airports_country flights.airports.country
    airports_country_abbrev flights.airports.country_abbrev
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
