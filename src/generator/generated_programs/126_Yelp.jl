using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("business_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("business_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "bid"], Any[0, "business id"], Any[0, "name"], Any[0, "full address"], Any[0, "city"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "review count"], Any[0, "is open"], Any[0, "rating"], Any[0, "state"], Any[1, "id"], Any[1, "business id"], Any[1, "category name"], Any[2, "uid"], Any[2, "user id"], Any[2, "name"], Any[3, "cid"], Any[3, "business id"], Any[3, "count"], Any[3, "day"], Any[4, "id"], Any[4, "business id"], Any[4, "neighbourhood name"], Any[5, "rid"], Any[5, "business id"], Any[5, "user id"], Any[5, "rating"], Any[5, "text"], Any[5, "year"], Any[5, "month"], Any[6, "tip id"], Any[6, "business id"], Any[6, "text"], Any[6, "user id"], Any[6, "likes"], Any[6, "year"], Any[6, "month"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "bid"], Any[0, "business id"], Any[0, "name"], Any[0, "full address"], Any[0, "city"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "review count"], Any[0, "is open"], Any[0, "rating"], Any[0, "state"], Any[1, "id"], Any[1, "business id"], Any[1, "category name"], Any[2, "uid"], Any[2, "user id"], Any[2, "name"], Any[3, "cid"], Any[3, "business id"], Any[3, "count"], Any[3, "day"], Any[4, "id"], Any[4, "business id"], Any[4, "neighbourhood name"], Any[5, "rid"], Any[5, "business id"], Any[5, "user id"], Any[5, "rating"], Any[5, "text"], Any[5, "year"], Any[5, "month"], Any[6, "tip id"], Any[6, "business id"], Any[6, "text"], Any[6, "user id"], Any[6, "likes"], Any[6, "year"], Any[6, "month"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["business id", "business id", "business id", "user id", "business id", "user id", "business id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "bid"], Any[0, "name"], Any[0, "full address"], Any[0, "city"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "review count"], Any[0, "is open"], Any[0, "rating"], Any[0, "state"], Any[1, "id"], Any[1, "category name"], Any[2, "uid"], Any[2, "name"], Any[3, "cid"], Any[3, "count"], Any[3, "day"], Any[4, "id"], Any[4, "neighbourhood name"], Any[5, "rid"], Any[5, "rating"], Any[5, "text"], Any[5, "year"], Any[5, "month"], Any[6, "tip id"], Any[6, "text"], Any[6, "likes"], Any[6, "year"], Any[6, "month"]]
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





PClean.@model YelpModel begin
    @class Business begin
        bid ~ Unmodeled()
        business_id ~ ChooseUniformly(possibilities[:business_id])
        name ~ ChooseUniformly(possibilities[:name])
        full_address ~ ChooseUniformly(possibilities[:full_address])
        city ~ ChooseUniformly(possibilities[:city])
        latitude ~ ChooseUniformly(possibilities[:latitude])
        longitude ~ ChooseUniformly(possibilities[:longitude])
        review_count ~ ChooseUniformly(possibilities[:review_count])
        is_open ~ ChooseUniformly(possibilities[:is_open])
        rating ~ ChooseUniformly(possibilities[:rating])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class User begin
        uid ~ Unmodeled()
        user_id ~ ChooseUniformly(possibilities[:user_id])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Obs begin
        business ~ Business
        user ~ User
        id ~ Unmodeled()
        category_name ~ ChooseUniformly(possibilities[:category_name])
        cid ~ Unmodeled()
        count ~ ChooseUniformly(possibilities[:count])
        day ~ ChooseUniformly(possibilities[:day])
        id ~ Unmodeled()
        neighbourhood_name ~ ChooseUniformly(possibilities[:neighbourhood_name])
        rid ~ Unmodeled()
        rating ~ ChooseUniformly(possibilities[:rating])
        text ~ ChooseUniformly(possibilities[:text])
        year ~ ChooseUniformly(possibilities[:year])
        month ~ ChooseUniformly(possibilities[:month])
        tip_id ~ Unmodeled()
        text ~ ChooseUniformly(possibilities[:text])
        likes ~ ChooseUniformly(possibilities[:likes])
        year ~ ChooseUniformly(possibilities[:year])
        month ~ ChooseUniformly(possibilities[:month])
    end
end

query = @query YelpModel.Obs [
    business_bid business.bid
    business_id business.business_id
    business_name business.name
    business_full_address business.full_address
    business_city business.city
    business_latitude business.latitude
    business_longitude business.longitude
    business_review_count business.review_count
    business_is_open business.is_open
    business_rating business.rating
    business_state business.state
    category_id id
    category_name category_name
    user_uid user.uid
    user_id user.user_id
    user_name user.name
    checkin_cid cid
    checkin_count count
    checkin_day day
    neighbourhood_id id
    neighbourhood_name neighbourhood_name
    review_rid rid
    review_rating rating
    review_text text
    review_year year
    review_month month
    tip_id tip_id
    tip_text text
    tip_likes likes
    tip_year year
    tip_month month
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
