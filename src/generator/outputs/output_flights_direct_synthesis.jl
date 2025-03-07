using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("datasets/flights_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("datasets/flights_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[0, "tuple_id"], Any[0, "src"], Any[0, "flight"], Any[0, "sched_dep_time"], Any[0, "act_dep_time"], Any[0, "sched_arr_time"], Any[0, "act_arr_time"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[0, "tuple_id"], Any[0, "src"], Any[0, "flight"], Any[0, "sched_dep_time"], Any[0, "act_dep_time"], Any[0, "sched_arr_time"], Any[0, "act_arr_time"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = Any[]
column_names_without_foreign_keys = Any[Any[0, "tuple_id"], Any[0, "src"], Any[0, "flight"], Any[0, "sched_dep_time"], Any[0, "act_dep_time"], Any[0, "sched_arr_time"], Any[0, "act_arr_time"]]
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

swap_possibilities = Dict()
swap_columns = Any[Any["sched_dep_time", Any["flight"], "src"], Any["act_dep_time", Any["flight"], "src"], Any["sched_arr_time", Any["flight"], "src"], Any["act_arr_time", Any["flight"], "src"]]
for swap_column in swap_columns
    swap_column_name = swap_column[1]
    same_identity_column_name = swap_column[2][1] 
    for r in eachrow(dirty_table)
        col_val = r[same_identity_column_name]
        swap_val = r[column_renaming_dict_reverse[swap_column_name]]
        key = "$(col_val)-$(swap_column_name)"
        if !ismissing(swap_val)
            if !(key in keys(swap_possibilities))
                swap_possibilities[key] = Set()
            end
            push!(swap_possibilities[key], swap_val)
        end
    end
end
swap_possibilities = Dict(c => [swap_possibilities[c]...] for c in keys(swap_possibilities))



PClean.@model FlightModel begin
    @class Time begin
        sched_dep_time ~ ChooseUniformly(possibilities[:sched_dep_time])
        act_dep_time ~ ChooseUniformly(possibilities[:act_dep_time])
        sched_arr_time ~ ChooseUniformly(possibilities[:sched_arr_time])
        act_arr_time ~ ChooseUniformly(possibilities[:act_arr_time])
    end

    @class Flight begin
        tuple_id ~ Unmodeled()
        src ~ ChooseUniformly(possibilities[:src])
        flight ~ ChooseUniformly(possibilities[:flight])
    end

    @class Obs begin
        @learned error_probs::Dict{String, ProbParameter{10.0, 50.0}}
        time ~ Time
        flight ~ Flight
        error_prob_flight = error_probs[flight.flight]
        sched_dep_time ~ MaybeSwap(time.sched_dep_time, swap_possibilities["$(flight.flight)-sched_dep"], error_prob_flight)
        act_dep_time ~ MaybeSwap(time.act_dep_time, swap_possibilities["$(flight.flight)-act_dep"], error_prob_flight)
        sched_arr_time ~ MaybeSwap(time.sched_arr_time, swap_possibilities["$(flight.flight)-sched_arr"], error_prob_flight)
        act_arr_time ~ MaybeSwap(time.act_arr_time, swap_possibilities["$(flight.flight)-act_arr"], error_prob_flight)
    end
end

query = @query FlightModel.Obs [
    tuple_id flight.tuple_id
    src flight.src
    flight flight.flight
    sched_dep_time time.sched_dep_time sched_dep_time
    act_dep_time time.act_dep_time act_dep_time
    sched_arr_time time.sched_arr_time sched_arr_time
    act_arr_time time.act_arr_time act_arr_time
]