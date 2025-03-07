using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("station_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("station_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "dock count"], Any[0, "city"], Any[0, "installation date"], Any[1, "station id"], Any[1, "bikes available"], Any[1, "docks available"], Any[1, "time"], Any[2, "id"], Any[2, "duration"], Any[2, "start date"], Any[2, "start station name"], Any[2, "start station id"], Any[2, "end date"], Any[2, "end station name"], Any[2, "end station id"], Any[2, "bike id"], Any[2, "subscription type"], Any[2, "zip code"], Any[3, "date"], Any[3, "max temperature f"], Any[3, "mean temperature f"], Any[3, "min temperature f"], Any[3, "max dew point f"], Any[3, "mean dew point f"], Any[3, "min dew point f"], Any[3, "max humidity"], Any[3, "mean humidity"], Any[3, "min humidity"], Any[3, "max sea level pressure inches"], Any[3, "mean sea level pressure inches"], Any[3, "min sea level pressure inches"], Any[3, "max visibility miles"], Any[3, "mean visibility miles"], Any[3, "min visibility miles"], Any[3, "max wind speed mph"], Any[3, "mean wind speed mph"], Any[3, "max gust speed mph"], Any[3, "precipitation inches"], Any[3, "cloud cover"], Any[3, "events"], Any[3, "wind dir degrees"], Any[3, "zip code"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "dock count"], Any[0, "city"], Any[0, "installation date"], Any[1, "station id"], Any[1, "bikes available"], Any[1, "docks available"], Any[1, "time"], Any[2, "id"], Any[2, "duration"], Any[2, "start date"], Any[2, "start station name"], Any[2, "start station id"], Any[2, "end date"], Any[2, "end station name"], Any[2, "end station id"], Any[2, "bike id"], Any[2, "subscription type"], Any[2, "zip code"], Any[3, "date"], Any[3, "max temperature f"], Any[3, "mean temperature f"], Any[3, "min temperature f"], Any[3, "max dew point f"], Any[3, "mean dew point f"], Any[3, "min dew point f"], Any[3, "max humidity"], Any[3, "mean humidity"], Any[3, "min humidity"], Any[3, "max sea level pressure inches"], Any[3, "mean sea level pressure inches"], Any[3, "min sea level pressure inches"], Any[3, "max visibility miles"], Any[3, "mean visibility miles"], Any[3, "min visibility miles"], Any[3, "max wind speed mph"], Any[3, "mean wind speed mph"], Any[3, "max gust speed mph"], Any[3, "precipitation inches"], Any[3, "cloud cover"], Any[3, "events"], Any[3, "wind dir degrees"], Any[3, "zip code"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "dock count"], Any[0, "city"], Any[0, "installation date"], Any[1, "station id"], Any[1, "bikes available"], Any[1, "docks available"], Any[1, "time"], Any[2, "id"], Any[2, "duration"], Any[2, "start date"], Any[2, "start station name"], Any[2, "start station id"], Any[2, "end date"], Any[2, "end station name"], Any[2, "end station id"], Any[2, "bike id"], Any[2, "subscription type"], Any[2, "zip code"], Any[3, "date"], Any[3, "max temperature f"], Any[3, "mean temperature f"], Any[3, "min temperature f"], Any[3, "max dew point f"], Any[3, "mean dew point f"], Any[3, "min dew point f"], Any[3, "max humidity"], Any[3, "mean humidity"], Any[3, "min humidity"], Any[3, "max sea level pressure inches"], Any[3, "mean sea level pressure inches"], Any[3, "min sea level pressure inches"], Any[3, "max visibility miles"], Any[3, "mean visibility miles"], Any[3, "min visibility miles"], Any[3, "max wind speed mph"], Any[3, "mean wind speed mph"], Any[3, "max gust speed mph"], Any[3, "precipitation inches"], Any[3, "cloud cover"], Any[3, "events"], Any[3, "wind dir degrees"], Any[3, "zip code"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "dock count"], Any[0, "city"], Any[0, "installation date"], Any[1, "station id"], Any[1, "bikes available"], Any[1, "docks available"], Any[1, "time"], Any[2, "id"], Any[2, "duration"], Any[2, "start date"], Any[2, "start station name"], Any[2, "start station id"], Any[2, "end date"], Any[2, "end station name"], Any[2, "end station id"], Any[2, "bike id"], Any[2, "subscription type"], Any[2, "zip code"], Any[3, "date"], Any[3, "max temperature f"], Any[3, "mean temperature f"], Any[3, "min temperature f"], Any[3, "max dew point f"], Any[3, "mean dew point f"], Any[3, "min dew point f"], Any[3, "max humidity"], Any[3, "mean humidity"], Any[3, "min humidity"], Any[3, "max sea level pressure inches"], Any[3, "mean sea level pressure inches"], Any[3, "min sea level pressure inches"], Any[3, "max visibility miles"], Any[3, "mean visibility miles"], Any[3, "min visibility miles"], Any[3, "max wind speed mph"], Any[3, "mean wind speed mph"], Any[3, "max gust speed mph"], Any[3, "precipitation inches"], Any[3, "cloud cover"], Any[3, "events"], Any[3, "wind dir degrees"], Any[3, "zip code"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "dock count"], Any[0, "city"], Any[0, "installation date"], Any[1, "station id"], Any[1, "bikes available"], Any[1, "docks available"], Any[1, "time"], Any[2, "id"], Any[2, "duration"], Any[2, "start date"], Any[2, "start station name"], Any[2, "start station id"], Any[2, "end date"], Any[2, "end station name"], Any[2, "end station id"], Any[2, "bike id"], Any[2, "subscription type"], Any[2, "zip code"], Any[3, "date"], Any[3, "max temperature f"], Any[3, "mean temperature f"], Any[3, "min temperature f"], Any[3, "max dew point f"], Any[3, "mean dew point f"], Any[3, "min dew point f"], Any[3, "max humidity"], Any[3, "mean humidity"], Any[3, "min humidity"], Any[3, "max sea level pressure inches"], Any[3, "mean sea level pressure inches"], Any[3, "min sea level pressure inches"], Any[3, "max visibility miles"], Any[3, "mean visibility miles"], Any[3, "min visibility miles"], Any[3, "max wind speed mph"], Any[3, "mean wind speed mph"], Any[3, "max gust speed mph"], Any[3, "precipitation inches"], Any[3, "cloud cover"], Any[3, "events"], Any[3, "wind dir degrees"], Any[3, "zip code"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[8, 1]])
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







PClean.@model Bike1Model begin
    @class Station begin
        name ~ ChooseUniformly(possibilities[:name])
        latitude ~ ChooseUniformly(possibilities[:latitude])
        longitude ~ ChooseUniformly(possibilities[:longitude])
        dock_count ~ ChooseUniformly(possibilities[:dock_count])
        city ~ ChooseUniformly(possibilities[:city])
        installation_date ~ ChooseUniformly(possibilities[:installation_date])
    end

    @class Status begin
        station ~ Station
        bikes_available ~ ChooseUniformly(possibilities[:bikes_available])
        docks_available ~ ChooseUniformly(possibilities[:docks_available])
        time ~ ChooseUniformly(possibilities[:time])
    end

    @class Trip begin
        duration ~ ChooseUniformly(possibilities[:duration])
        start_date ~ ChooseUniformly(possibilities[:start_date])
        start_station_name ~ ChooseUniformly(possibilities[:start_station_name])
        start_station_id ~ ChooseUniformly(possibilities[:start_station_id])
        end_date ~ ChooseUniformly(possibilities[:end_date])
        end_station_name ~ ChooseUniformly(possibilities[:end_station_name])
        end_station_id ~ ChooseUniformly(possibilities[:end_station_id])
        bike_id ~ ChooseUniformly(possibilities[:bike_id])
        subscription_type ~ ChooseUniformly(possibilities[:subscription_type])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
    end

    @class Weather begin
        date ~ ChooseUniformly(possibilities[:date])
        max_temperature_f ~ ChooseUniformly(possibilities[:max_temperature_f])
        mean_temperature_f ~ ChooseUniformly(possibilities[:mean_temperature_f])
        min_temperature_f ~ ChooseUniformly(possibilities[:min_temperature_f])
        max_dew_point_f ~ ChooseUniformly(possibilities[:max_dew_point_f])
        mean_dew_point_f ~ ChooseUniformly(possibilities[:mean_dew_point_f])
        min_dew_point_f ~ ChooseUniformly(possibilities[:min_dew_point_f])
        max_humidity ~ ChooseUniformly(possibilities[:max_humidity])
        mean_humidity ~ ChooseUniformly(possibilities[:mean_humidity])
        min_humidity ~ ChooseUniformly(possibilities[:min_humidity])
        max_sea_level_pressure_inches ~ ChooseUniformly(possibilities[:max_sea_level_pressure_inches])
        mean_sea_level_pressure_inches ~ ChooseUniformly(possibilities[:mean_sea_level_pressure_inches])
        min_sea_level_pressure_inches ~ ChooseUniformly(possibilities[:min_sea_level_pressure_inches])
        max_visibility_miles ~ ChooseUniformly(possibilities[:max_visibility_miles])
        mean_visibility_miles ~ ChooseUniformly(possibilities[:mean_visibility_miles])
        min_visibility_miles ~ ChooseUniformly(possibilities[:min_visibility_miles])
        max_wind_speed_mph ~ ChooseUniformly(possibilities[:max_wind_speed_mph])
        mean_wind_speed_mph ~ ChooseUniformly(possibilities[:mean_wind_speed_mph])
        max_gust_speed_mph ~ ChooseUniformly(possibilities[:max_gust_speed_mph])
        precipitation_inches ~ ChooseUniformly(possibilities[:precipitation_inches])
        cloud_cover ~ ChooseUniformly(possibilities[:cloud_cover])
        events ~ ChooseUniformly(possibilities[:events])
        wind_dir_degrees ~ ChooseUniformly(possibilities[:wind_dir_degrees])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
    end

    @class Obs begin
        status ~ Status
        trip ~ Trip
        weather ~ Weather
    end
end

query = @query Bike1Model.Obs [
    station_name status.station.name
    station_latitude status.station.latitude
    station_longitude status.station.longitude
    station_dock_count status.station.dock_count
    station_city status.station.city
    station_installation_date status.station.installation_date
    status_bikes_available status.bikes_available
    status_docks_available status.docks_available
    status_time status.time
    trip_duration trip.duration
    trip_start_date trip.start_date
    trip_start_station_name trip.start_station_name
    trip_start_station_id trip.start_station_id
    trip_end_date trip.end_date
    trip_end_station_name trip.end_station_name
    trip_end_station_id trip.end_station_id
    trip_bike_id trip.bike_id
    trip_subscription_type trip.subscription_type
    trip_zip_code trip.zip_code
    weather_date weather.date
    weather_max_temperature_f weather.max_temperature_f
    weather_mean_temperature_f weather.mean_temperature_f
    weather_min_temperature_f weather.min_temperature_f
    weather_max_dew_point_f weather.max_dew_point_f
    weather_mean_dew_point_f weather.mean_dew_point_f
    weather_min_dew_point_f weather.min_dew_point_f
    weather_max_humidity weather.max_humidity
    weather_mean_humidity weather.mean_humidity
    weather_min_humidity weather.min_humidity
    weather_max_sea_level_pressure_inches weather.max_sea_level_pressure_inches
    weather_mean_sea_level_pressure_inches weather.mean_sea_level_pressure_inches
    weather_min_sea_level_pressure_inches weather.min_sea_level_pressure_inches
    weather_max_visibility_miles weather.max_visibility_miles
    weather_mean_visibility_miles weather.mean_visibility_miles
    weather_min_visibility_miles weather.min_visibility_miles
    weather_max_wind_speed_mph weather.max_wind_speed_mph
    weather_mean_wind_speed_mph weather.mean_wind_speed_mph
    weather_max_gust_speed_mph weather.max_gust_speed_mph
    weather_precipitation_inches weather.precipitation_inches
    weather_cloud_cover weather.cloud_cover
    weather_events weather.events
    weather_wind_dir_degrees weather.wind_dir_degrees
    weather_zip_code weather.zip_code
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
