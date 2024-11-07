using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("follows_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("follows_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "user id"], Any[0, "follower id"], Any[1, "id"], Any[1, "user id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "user id"], Any[0, "follower id"], Any[1, "id"], Any[1, "user id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Twitter1Model begin
    @class Follows begin
        user_id ~ Unmodeled()
        follower_id ~ ChooseUniformly(possibilities[:follower_id])
    end

    @class Tweets begin
        id ~ Unmodeled()
        user_id ~ ChooseUniformly(possibilities[:user_id])
        text ~ ChooseUniformly(possibilities[:text])
        create_date ~ TimePrior(possibilities[:create_date])
    end

    @class User_Profiles begin
        uid ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        email ~ ChooseUniformly(possibilities[:email])
        partition_id ~ ChooseUniformly(possibilities[:partition_id])
        followers ~ ChooseUniformly(possibilities[:followers])
    end

    @class Obs begin
        follows ~ Follows
        tweets ~ Tweets
        user_Profiles ~ User_Profiles
    end
end

query = @query Twitter1Model.Obs [
    tweets_id tweets.id
    tweets_text tweets.text
    tweets_create_date tweets.create_date
    user_profiles_uid user_Profiles.uid
    user_profiles_name user_Profiles.name
    user_profiles_email user_Profiles.email
    user_profiles_partition_id user_Profiles.partition_id
    user_profiles_followers user_Profiles.followers
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
