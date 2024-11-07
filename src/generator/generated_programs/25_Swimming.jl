using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("swimmer_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("swimmer_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "meter 100"], Any[0, "meter 200"], Any[0, "meter 300"], Any[0, "meter 400"], Any[0, "meter 500"], Any[0, "meter 600"], Any[0, "meter 700"], Any[0, "time"], Any[1, "id"], Any[1, "name"], Any[1, "capacity"], Any[1, "city"], Any[1, "country"], Any[1, "opening year"], Any[2, "id"], Any[2, "name"], Any[2, "stadium id"], Any[2, "year"], Any[3, "id"], Any[3, "result"], Any[3, "swimmer id"], Any[3, "event id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "meter 100"], Any[0, "meter 200"], Any[0, "meter 300"], Any[0, "meter 400"], Any[0, "meter 500"], Any[0, "meter 600"], Any[0, "meter 700"], Any[0, "time"], Any[1, "id"], Any[1, "name"], Any[1, "capacity"], Any[1, "city"], Any[1, "country"], Any[1, "opening year"], Any[2, "id"], Any[2, "name"], Any[2, "stadium id"], Any[2, "year"], Any[3, "id"], Any[3, "result"], Any[3, "swimmer id"], Any[3, "event id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model SwimmingModel begin
    @class Swimmer begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        meter_100 ~ ChooseUniformly(possibilities[:meter_100])
        meter_200 ~ ChooseUniformly(possibilities[:meter_200])
        meter_300 ~ ChooseUniformly(possibilities[:meter_300])
        meter_400 ~ ChooseUniformly(possibilities[:meter_400])
        meter_500 ~ ChooseUniformly(possibilities[:meter_500])
        meter_600 ~ ChooseUniformly(possibilities[:meter_600])
        meter_700 ~ ChooseUniformly(possibilities[:meter_700])
        time ~ ChooseUniformly(possibilities[:time])
    end

    @class Stadium begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        capacity ~ ChooseUniformly(possibilities[:capacity])
        city ~ ChooseUniformly(possibilities[:city])
        country ~ ChooseUniformly(possibilities[:country])
        opening_year ~ ChooseUniformly(possibilities[:opening_year])
    end

    @class Event begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        stadium_id ~ ChooseUniformly(possibilities[:stadium_id])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Record begin
        id ~ Unmodeled()
        result ~ ChooseUniformly(possibilities[:result])
        swimmer_id ~ ChooseUniformly(possibilities[:swimmer_id])
        event_id ~ ChooseUniformly(possibilities[:event_id])
    end

    @class Obs begin
        swimmer ~ Swimmer
        stadium ~ Stadium
        event ~ Event
        record ~ Record
    end
end

query = @query SwimmingModel.Obs [
    swimmer_id swimmer.id
    swimmer_name swimmer.name
    swimmer_nationality swimmer.nationality
    swimmer_meter_100 swimmer.meter_100
    swimmer_meter_200 swimmer.meter_200
    swimmer_meter_300 swimmer.meter_300
    swimmer_meter_400 swimmer.meter_400
    swimmer_meter_500 swimmer.meter_500
    swimmer_meter_600 swimmer.meter_600
    swimmer_meter_700 swimmer.meter_700
    swimmer_time swimmer.time
    stadium_id stadium.id
    stadium_name stadium.name
    stadium_capacity stadium.capacity
    stadium_city stadium.city
    stadium_country stadium.country
    stadium_opening_year stadium.opening_year
    event_id event.id
    event_name event.name
    event_year event.year
    record_id record.id
    record_result record.result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
