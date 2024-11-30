using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("train_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("train_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["station id", "train id", "station id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "train number"], Any[0, "name"], Any[0, "origin"], Any[0, "destination"], Any[0, "time"], Any[0, "interval"], Any[1, "id"], Any[1, "network name"], Any[1, "services"], Any[1, "local authority"], Any[3, "day of week"], Any[3, "high temperature"], Any[3, "low temperature"], Any[3, "precipitation"], Any[3, "wind speed mph"]]
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





PClean.@model StationWeatherModel begin
    @class Train begin
        id ~ Unmodeled()
        train_number ~ ChooseUniformly(possibilities[:train_number])
        name ~ ChooseUniformly(possibilities[:name])
        origin ~ ChooseUniformly(possibilities[:origin])
        destination ~ ChooseUniformly(possibilities[:destination])
        time ~ ChooseUniformly(possibilities[:time])
        interval ~ ChooseUniformly(possibilities[:interval])
    end

    @class Station begin
        id ~ Unmodeled()
        network_name ~ ChooseUniformly(possibilities[:network_name])
        services ~ ChooseUniformly(possibilities[:services])
        local_authority ~ ChooseUniformly(possibilities[:local_authority])
    end

    @class Obs begin
        train ~ Train
        station ~ Station
        day_of_week ~ ChooseUniformly(possibilities[:day_of_week])
        high_temperature ~ ChooseUniformly(possibilities[:high_temperature])
        low_temperature ~ ChooseUniformly(possibilities[:low_temperature])
        precipitation ~ ChooseUniformly(possibilities[:precipitation])
        wind_speed_mph ~ ChooseUniformly(possibilities[:wind_speed_mph])
    end
end

query = @query StationWeatherModel.Obs [
    train_id train.id
    train_number train.train_number
    train_name train.name
    train_origin train.origin
    train_destination train.destination
    train_time train.time
    train_interval train.interval
    station_id station.id
    station_network_name station.network_name
    station_services station.services
    station_local_authority station.local_authority
    weekly_weather_day_of_week day_of_week
    weekly_weather_high_temperature high_temperature
    weekly_weather_low_temperature low_temperature
    weekly_weather_precipitation precipitation
    weekly_weather_wind_speed_mph wind_speed_mph
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
