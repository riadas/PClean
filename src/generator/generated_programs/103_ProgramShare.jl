using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("program_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("program_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "program id"], Any[0, "name"], Any[0, "origin"], Any[0, "launch"], Any[0, "owner"], Any[1, "channel id"], Any[1, "name"], Any[1, "owner"], Any[1, "share in percent"], Any[1, "rating in percent"], Any[2, "channel id"], Any[2, "program id"], Any[2, "time of day"], Any[3, "channel id"], Any[3, "program id"], Any[3, "date"], Any[3, "share in percent"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "program id"], Any[0, "name"], Any[0, "origin"], Any[0, "launch"], Any[0, "owner"], Any[1, "channel id"], Any[1, "name"], Any[1, "owner"], Any[1, "share in percent"], Any[1, "rating in percent"], Any[2, "channel id"], Any[2, "program id"], Any[2, "time of day"], Any[3, "channel id"], Any[3, "program id"], Any[3, "date"], Any[3, "share in percent"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["program id", "channel id", "program id", "channel id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "origin"], Any[0, "launch"], Any[0, "owner"], Any[1, "name"], Any[1, "owner"], Any[1, "share in percent"], Any[1, "rating in percent"], Any[2, "time of day"], Any[3, "date"], Any[3, "share in percent"]]
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





PClean.@model ProgramShareModel begin
    @class Program begin
        program_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        origin ~ ChooseUniformly(possibilities[:origin])
        launch ~ ChooseUniformly(possibilities[:launch])
        owner ~ ChooseUniformly(possibilities[:owner])
    end

    @class Channel begin
        channel_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        owner ~ ChooseUniformly(possibilities[:owner])
        share_in_percent ~ ChooseUniformly(possibilities[:share_in_percent])
        rating_in_percent ~ ChooseUniformly(possibilities[:rating_in_percent])
    end

    @class Obs begin
        program ~ Program
        channel ~ Channel
        time_of_day ~ ChooseUniformly(possibilities[:time_of_day])
        date ~ ChooseUniformly(possibilities[:date])
        share_in_percent ~ ChooseUniformly(possibilities[:share_in_percent])
    end
end

query = @query ProgramShareModel.Obs [
    program_id program.program_id
    program_name program.name
    program_origin program.origin
    program_launch program.launch
    program_owner program.owner
    channel_id channel.channel_id
    channel_name channel.name
    channel_owner channel.owner
    channel_share_in_percent channel.share_in_percent
    channel_rating_in_percent channel.rating_in_percent
    broadcast_time_of_day time_of_day
    broadcast_share_date date
    broadcast_share_share_in_percent share_in_percent
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
