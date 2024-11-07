using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("station_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("station_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "station id"], Any[0, "name"], Any[0, "annual entry exit"], Any[0, "annual interchanges"], Any[0, "total passengers"], Any[0, "location"], Any[0, "main services"], Any[0, "number of platforms"], Any[1, "train id"], Any[1, "name"], Any[1, "time"], Any[1, "service"], Any[2, "train id"], Any[2, "station id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "station id"], Any[0, "name"], Any[0, "annual entry exit"], Any[0, "annual interchanges"], Any[0, "total passengers"], Any[0, "location"], Any[0, "main services"], Any[0, "number of platforms"], Any[1, "train id"], Any[1, "name"], Any[1, "time"], Any[1, "service"], Any[2, "train id"], Any[2, "station id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model TrainStationModel begin
    @class Station begin
        station_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        annual_entry_exit ~ ChooseUniformly(possibilities[:annual_entry_exit])
        annual_interchanges ~ ChooseUniformly(possibilities[:annual_interchanges])
        total_passengers ~ ChooseUniformly(possibilities[:total_passengers])
        location ~ ChooseUniformly(possibilities[:location])
        main_services ~ ChooseUniformly(possibilities[:main_services])
        number_of_platforms ~ ChooseUniformly(possibilities[:number_of_platforms])
    end

    @class Train begin
        train_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        time ~ ChooseUniformly(possibilities[:time])
        service ~ ChooseUniformly(possibilities[:service])
    end

    @class Train_Station begin
        train_id ~ Unmodeled()
        station_id ~ ChooseUniformly(possibilities[:station_id])
    end

    @class Obs begin
        station ~ Station
        train ~ Train
        train_Station ~ Train_Station
    end
end

query = @query TrainStationModel.Obs [
    station_id station.station_id
    station_name station.name
    station_annual_entry_exit station.annual_entry_exit
    station_annual_interchanges station.annual_interchanges
    station_total_passengers station.total_passengers
    station_location station.location
    station_main_services station.main_services
    station_number_of_platforms station.number_of_platforms
    train_id train.train_id
    train_name train.name
    train_time train.time
    train_service train.service
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
