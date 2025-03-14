using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("airlines_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("airlines_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["destination airport", "source airport"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "airline id"], Any[0, "airline name"], Any[0, "abbreviation"], Any[0, "country"], Any[1, "city"], Any[1, "airport code"], Any[1, "airport name"], Any[1, "country"], Any[1, "country abbrev"], Any[2, "airline"], Any[2, "flight number"]]
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

    @class Obs begin
        airlines ~ Airlines
        airports ~ Airports
        airline ~ ChooseUniformly(possibilities[:airline])
        flight_number ~ ChooseUniformly(possibilities[:flight_number])
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
    flights_airline airline
    flights_flight_number flight_number
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
