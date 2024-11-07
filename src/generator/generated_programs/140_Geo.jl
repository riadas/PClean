using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("state_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("state_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "state name"], Any[0, "population"], Any[0, "area"], Any[0, "country name"], Any[0, "capital"], Any[0, "density"], Any[1, "city name"], Any[1, "population"], Any[1, "country name"], Any[1, "state name"], Any[2, "state name"], Any[2, "border"], Any[3, "state name"], Any[3, "highest elevation"], Any[3, "lowest point"], Any[3, "highest point"], Any[3, "lowest elevation"], Any[4, "lake name"], Any[4, "area"], Any[4, "country name"], Any[4, "state name"], Any[5, "mountain name"], Any[5, "mountain altitude"], Any[5, "country name"], Any[5, "state name"], Any[6, "river name"], Any[6, "length"], Any[6, "country name"], Any[6, "traverse"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "state name"], Any[0, "population"], Any[0, "area"], Any[0, "country name"], Any[0, "capital"], Any[0, "density"], Any[1, "city name"], Any[1, "population"], Any[1, "country name"], Any[1, "state name"], Any[2, "state name"], Any[2, "border"], Any[3, "state name"], Any[3, "highest elevation"], Any[3, "lowest point"], Any[3, "highest point"], Any[3, "lowest elevation"], Any[4, "lake name"], Any[4, "area"], Any[4, "country name"], Any[4, "state name"], Any[5, "mountain name"], Any[5, "mountain altitude"], Any[5, "country name"], Any[5, "state name"], Any[6, "river name"], Any[6, "length"], Any[6, "country name"], Any[6, "traverse"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model GeoModel begin
    @class State begin
        state_name ~ ChooseUniformly(possibilities[:state_name])
        population ~ ChooseUniformly(possibilities[:population])
        area ~ ChooseUniformly(possibilities[:area])
        country_name ~ ChooseUniformly(possibilities[:country_name])
        capital ~ ChooseUniformly(possibilities[:capital])
        density ~ ChooseUniformly(possibilities[:density])
    end

    @class City begin
        city_name ~ ChooseUniformly(possibilities[:city_name])
        population ~ ChooseUniformly(possibilities[:population])
        country_name ~ ChooseUniformly(possibilities[:country_name])
        state_name ~ ChooseUniformly(possibilities[:state_name])
    end

    @class Border_Info begin
        state_name ~ ChooseUniformly(possibilities[:state_name])
        border ~ ChooseUniformly(possibilities[:border])
    end

    @class Highlow begin
        state_name ~ ChooseUniformly(possibilities[:state_name])
        highest_elevation ~ ChooseUniformly(possibilities[:highest_elevation])
        lowest_point ~ ChooseUniformly(possibilities[:lowest_point])
        highest_point ~ ChooseUniformly(possibilities[:highest_point])
        lowest_elevation ~ ChooseUniformly(possibilities[:lowest_elevation])
    end

    @class Lake begin
        lake_name ~ ChooseUniformly(possibilities[:lake_name])
        area ~ ChooseUniformly(possibilities[:area])
        country_name ~ ChooseUniformly(possibilities[:country_name])
        state_name ~ ChooseUniformly(possibilities[:state_name])
    end

    @class Mountain begin
        mountain_name ~ ChooseUniformly(possibilities[:mountain_name])
        mountain_altitude ~ ChooseUniformly(possibilities[:mountain_altitude])
        country_name ~ ChooseUniformly(possibilities[:country_name])
        state_name ~ ChooseUniformly(possibilities[:state_name])
    end

    @class River begin
        river_name ~ ChooseUniformly(possibilities[:river_name])
        length ~ ChooseUniformly(possibilities[:length])
        country_name ~ ChooseUniformly(possibilities[:country_name])
        traverse ~ ChooseUniformly(possibilities[:traverse])
    end

    @class Obs begin
        state ~ State
        city ~ City
        border_Info ~ Border_Info
        highlow ~ Highlow
        lake ~ Lake
        mountain ~ Mountain
        river ~ River
    end
end

query = @query GeoModel.Obs [
    state_name state.state_name
    state_population state.population
    state_area state.area
    state_country_name state.country_name
    state_capital state.capital
    state_density state.density
    city_name city.city_name
    city_population city.population
    city_country_name city.country_name
    highlow_highest_elevation highlow.highest_elevation
    highlow_lowest_point highlow.lowest_point
    highlow_highest_point highlow.highest_point
    highlow_lowest_elevation highlow.lowest_elevation
    lake_name lake.lake_name
    lake_area lake.area
    lake_country_name lake.country_name
    lake_state_name lake.state_name
    mountain_name mountain.mountain_name
    mountain_altitude mountain.mountain_altitude
    mountain_country_name mountain.country_name
    river_name river.river_name
    river_length river.length
    river_country_name river.country_name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
