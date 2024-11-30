using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("follows_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("follows_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["follower id", "user id", "user id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[1, "id"], Any[1, "text"], Any[1, "create date"], Any[2, "uid"], Any[2, "name"], Any[2, "email"], Any[2, "partition id"], Any[2, "followers"]]
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





PClean.@model Twitter1Model begin
    @class User_Profiles begin
        uid ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        email ~ ChooseUniformly(possibilities[:email])
        partition_id ~ ChooseUniformly(possibilities[:partition_id])
        followers ~ ChooseUniformly(possibilities[:followers])
    end

    @class Obs begin
        user_Profiles ~ User_Profiles
        id ~ Unmodeled()
        text ~ ChooseUniformly(possibilities[:text])
        create_date ~ TimePrior(possibilities[:create_date])
    end
end

query = @query Twitter1Model.Obs [
    tweets_id id
    tweets_text text
    tweets_create_date create_date
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

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
