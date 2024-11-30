using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("swimmer_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("swimmer_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "meter 100"], Any[0, "meter 200"], Any[0, "meter 300"], Any[0, "meter 400"], Any[0, "meter 500"], Any[0, "meter 600"], Any[0, "meter 700"], Any[0, "time"], Any[1, "id"], Any[1, "name"], Any[1, "capacity"], Any[1, "city"], Any[1, "country"], Any[1, "opening year"], Any[2, "id"], Any[2, "name"], Any[2, "stadium id"], Any[2, "year"], Any[3, "id"], Any[3, "result"], Any[3, "swimmer id"], Any[3, "event id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "meter 100"], Any[0, "meter 200"], Any[0, "meter 300"], Any[0, "meter 400"], Any[0, "meter 500"], Any[0, "meter 600"], Any[0, "meter 700"], Any[0, "time"], Any[1, "id"], Any[1, "name"], Any[1, "capacity"], Any[1, "city"], Any[1, "country"], Any[1, "opening year"], Any[2, "id"], Any[2, "name"], Any[2, "stadium id"], Any[2, "year"], Any[3, "id"], Any[3, "result"], Any[3, "swimmer id"], Any[3, "event id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["stadium id", "swimmer id", "event id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "meter 100"], Any[0, "meter 200"], Any[0, "meter 300"], Any[0, "meter 400"], Any[0, "meter 500"], Any[0, "meter 600"], Any[0, "meter 700"], Any[0, "time"], Any[1, "id"], Any[1, "name"], Any[1, "capacity"], Any[1, "city"], Any[1, "country"], Any[1, "opening year"], Any[2, "id"], Any[2, "name"], Any[2, "year"], Any[3, "id"], Any[3, "result"]]
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
        stadium ~ Stadium
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Record begin
        id ~ Unmodeled()
        result ~ ChooseUniformly(possibilities[:result])
        swimmer ~ Swimmer
        event ~ Event
    end

    @class Obs begin
        record ~ Record
    end
end

query = @query SwimmingModel.Obs [
    swimmer_id record.swimmer.id
    swimmer_name record.swimmer.name
    swimmer_nationality record.swimmer.nationality
    swimmer_meter_100 record.swimmer.meter_100
    swimmer_meter_200 record.swimmer.meter_200
    swimmer_meter_300 record.swimmer.meter_300
    swimmer_meter_400 record.swimmer.meter_400
    swimmer_meter_500 record.swimmer.meter_500
    swimmer_meter_600 record.swimmer.meter_600
    swimmer_meter_700 record.swimmer.meter_700
    swimmer_time record.swimmer.time
    stadium_id record.event.stadium.id
    stadium_name record.event.stadium.name
    stadium_capacity record.event.stadium.capacity
    stadium_city record.event.stadium.city
    stadium_country record.event.stadium.country
    stadium_opening_year record.event.stadium.opening_year
    event_id record.event.id
    event_name record.event.name
    event_year record.event.year
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
