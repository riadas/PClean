using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("train_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("train_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "train number"], Any[0, "name"], Any[0, "origin"], Any[0, "destination"], Any[0, "time"], Any[0, "interval"], Any[1, "id"], Any[1, "network name"], Any[1, "services"], Any[1, "local authority"], Any[2, "train id"], Any[2, "station id"], Any[3, "station id"], Any[3, "day of week"], Any[3, "high temperature"], Any[3, "low temperature"], Any[3, "precipitation"], Any[3, "wind speed mph"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "train number"], Any[0, "name"], Any[0, "origin"], Any[0, "destination"], Any[0, "time"], Any[0, "interval"], Any[1, "id"], Any[1, "network name"], Any[1, "services"], Any[1, "local authority"], Any[2, "train id"], Any[2, "station id"], Any[3, "station id"], Any[3, "day of week"], Any[3, "high temperature"], Any[3, "low temperature"], Any[3, "precipitation"], Any[3, "wind speed mph"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "train number"], Any[0, "name"], Any[0, "origin"], Any[0, "destination"], Any[0, "time"], Any[0, "interval"], Any[1, "id"], Any[1, "network name"], Any[1, "services"], Any[1, "local authority"], Any[2, "train id"], Any[2, "station id"], Any[3, "station id"], Any[3, "day of week"], Any[3, "high temperature"], Any[3, "low temperature"], Any[3, "precipitation"], Any[3, "wind speed mph"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "train number"], Any[0, "name"], Any[0, "origin"], Any[0, "destination"], Any[0, "time"], Any[0, "interval"], Any[1, "id"], Any[1, "network name"], Any[1, "services"], Any[1, "local authority"], Any[2, "train id"], Any[2, "station id"], Any[3, "station id"], Any[3, "day of week"], Any[3, "high temperature"], Any[3, "low temperature"], Any[3, "precipitation"], Any[3, "wind speed mph"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "train number"], Any[0, "name"], Any[0, "origin"], Any[0, "destination"], Any[0, "time"], Any[0, "interval"], Any[1, "id"], Any[1, "network name"], Any[1, "services"], Any[1, "local authority"], Any[2, "train id"], Any[2, "station id"], Any[3, "station id"], Any[3, "day of week"], Any[3, "high temperature"], Any[3, "low temperature"], Any[3, "precipitation"], Any[3, "wind speed mph"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 8], Any[12, 1], Any[14, 8]])
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







PClean.@model StationWeatherModel begin
    @class Train begin
        train_number ~ ChooseUniformly(possibilities[:train_number])
        name ~ ChooseUniformly(possibilities[:name])
        origin ~ ChooseUniformly(possibilities[:origin])
        destination ~ ChooseUniformly(possibilities[:destination])
        time ~ ChooseUniformly(possibilities[:time])
        interval ~ ChooseUniformly(possibilities[:interval])
    end

    @class Station begin
        network_name ~ ChooseUniformly(possibilities[:network_name])
        services ~ ChooseUniformly(possibilities[:services])
        local_authority ~ ChooseUniformly(possibilities[:local_authority])
    end

    @class Route begin
        station ~ Station
    end

    @class Weekly_weather begin
        day_of_week ~ ChooseUniformly(possibilities[:day_of_week])
        high_temperature ~ ChooseUniformly(possibilities[:high_temperature])
        low_temperature ~ ChooseUniformly(possibilities[:low_temperature])
        precipitation ~ ChooseUniformly(possibilities[:precipitation])
        wind_speed_mph ~ ChooseUniformly(possibilities[:wind_speed_mph])
    end

    @class Obs begin
        route ~ Route
        weekly_weather ~ Weekly_weather
    end
end

query = @query StationWeatherModel.Obs [
    train_number route.train.train_number
    train_name route.train.name
    train_origin route.train.origin
    train_destination route.train.destination
    train_time route.train.time
    train_interval route.train.interval
    station_network_name route.station.network_name
    station_services route.station.services
    station_local_authority route.station.local_authority
    weekly_weather_day_of_week weekly_weather.day_of_week
    weekly_weather_high_temperature weekly_weather.high_temperature
    weekly_weather_low_temperature weekly_weather.low_temperature
    weekly_weather_precipitation weekly_weather.precipitation
    weekly_weather_wind_speed_mph weekly_weather.wind_speed_mph
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
