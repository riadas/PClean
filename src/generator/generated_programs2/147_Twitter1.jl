using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("follows_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("follows_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "user id"], Any[0, "follower id"], Any[1, "id"], Any[1, "user id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "user id"], Any[0, "follower id"], Any[1, "id"], Any[1, "user id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "user id"], Any[0, "follower id"], Any[1, "id"], Any[1, "user id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "user id"], Any[0, "follower id"], Any[1, "id"], Any[1, "user id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "user id"], Any[0, "follower id"], Any[1, "id"], Any[1, "user id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[2, 7], Any[1, 7], Any[4, 7]])
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







PClean.@model Twitter1Model begin
    @class User_profiles begin
        name ~ ChooseUniformly(possibilities[:name])
        email ~ ChooseUniformly(possibilities[:email])
        partition_id ~ ChooseUniformly(possibilities[:partition_id])
        followers ~ ChooseUniformly(possibilities[:followers])
    end

    @class Follows begin
        user_profiles ~ User_profiles
    end

    @class Tweets begin
        user_profiles ~ User_profiles
        text ~ ChooseUniformly(possibilities[:text])
        create_date ~ TimePrior(possibilities[:create_date])
    end

    @class Obs begin
        follows ~ Follows
        tweets ~ Tweets
    end
end

query = @query Twitter1Model.Obs [
    tweets_text tweets.text
    tweets_create_date tweets.create_date
    user_profiles_name follows.user_profiles.name
    user_profiles_email follows.user_profiles.email
    user_profiles_partition_id follows.user_profiles.partition_id
    user_profiles_followers follows.user_profiles.followers
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
