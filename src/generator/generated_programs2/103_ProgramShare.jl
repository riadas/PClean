using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("program_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("program_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "program id"], Any[0, "name"], Any[0, "origin"], Any[0, "launch"], Any[0, "owner"], Any[1, "channel id"], Any[1, "name"], Any[1, "owner"], Any[1, "share in percent"], Any[1, "rating in percent"], Any[2, "channel id"], Any[2, "program id"], Any[2, "time of day"], Any[3, "channel id"], Any[3, "program id"], Any[3, "date"], Any[3, "share in percent"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 1], Any[11, 6], Any[15, 1], Any[14, 6]])
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







PClean.@model ProgramShareModel begin
    @class Program begin
        name ~ ChooseUniformly(possibilities[:name])
        origin ~ ChooseUniformly(possibilities[:origin])
        launch ~ ChooseUniformly(possibilities[:launch])
        owner ~ ChooseUniformly(possibilities[:owner])
    end

    @class Channel begin
        name ~ ChooseUniformly(possibilities[:name])
        owner ~ ChooseUniformly(possibilities[:owner])
        share_in_percent ~ ChooseUniformly(possibilities[:share_in_percent])
        rating_in_percent ~ ChooseUniformly(possibilities[:rating_in_percent])
    end

    @class Broadcast begin
        program ~ Program
        time_of_day ~ ChooseUniformly(possibilities[:time_of_day])
    end

    @class Broadcast_share begin
        program ~ Program
        date ~ ChooseUniformly(possibilities[:date])
        share_in_percent ~ ChooseUniformly(possibilities[:share_in_percent])
    end

    @class Obs begin
        broadcast ~ Broadcast
        broadcast_share ~ Broadcast_share
    end
end

query = @query ProgramShareModel.Obs [
    program_id broadcast.program.program_id
    program_name broadcast.program.name
    program_origin broadcast.program.origin
    program_launch broadcast.program.launch
    program_owner broadcast.program.owner
    channel_id broadcast.channel.channel_id
    channel_name broadcast.channel.name
    channel_owner broadcast.channel.owner
    channel_share_in_percent broadcast.channel.share_in_percent
    channel_rating_in_percent broadcast.channel.rating_in_percent
    broadcast_time_of_day broadcast.time_of_day
    broadcast_share_date broadcast_share.date
    broadcast_share_share_in_percent broadcast_share.share_in_percent
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
