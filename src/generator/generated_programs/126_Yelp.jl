using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("business_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("business_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "bid"], Any[0, "business id"], Any[0, "name"], Any[0, "full address"], Any[0, "city"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "review count"], Any[0, "is open"], Any[0, "rating"], Any[0, "state"], Any[1, "id"], Any[1, "business id"], Any[1, "category name"], Any[2, "uid"], Any[2, "user id"], Any[2, "name"], Any[3, "cid"], Any[3, "business id"], Any[3, "count"], Any[3, "day"], Any[4, "id"], Any[4, "business id"], Any[4, "neighbourhood name"], Any[5, "rid"], Any[5, "business id"], Any[5, "user id"], Any[5, "rating"], Any[5, "text"], Any[5, "year"], Any[5, "month"], Any[6, "tip id"], Any[6, "business id"], Any[6, "text"], Any[6, "user id"], Any[6, "likes"], Any[6, "year"], Any[6, "month"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "bid"], Any[0, "business id"], Any[0, "name"], Any[0, "full address"], Any[0, "city"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "review count"], Any[0, "is open"], Any[0, "rating"], Any[0, "state"], Any[1, "id"], Any[1, "business id"], Any[1, "category name"], Any[2, "uid"], Any[2, "user id"], Any[2, "name"], Any[3, "cid"], Any[3, "business id"], Any[3, "count"], Any[3, "day"], Any[4, "id"], Any[4, "business id"], Any[4, "neighbourhood name"], Any[5, "rid"], Any[5, "business id"], Any[5, "user id"], Any[5, "rating"], Any[5, "text"], Any[5, "year"], Any[5, "month"], Any[6, "tip id"], Any[6, "business id"], Any[6, "text"], Any[6, "user id"], Any[6, "likes"], Any[6, "year"], Any[6, "month"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Category begin
        id ~ Unmodeled()
        business_id ~ ChooseUniformly(possibilities[:business_id])
        category_name ~ ChooseUniformly(possibilities[:category_name])
    end

    @class User begin
        uid ~ Unmodeled()
        user_id ~ ChooseUniformly(possibilities[:user_id])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Checkin begin
        cid ~ Unmodeled()
        business_id ~ ChooseUniformly(possibilities[:business_id])
        count ~ ChooseUniformly(possibilities[:count])
        day ~ ChooseUniformly(possibilities[:day])
    end

    @class Neighbourhood begin
        id ~ Unmodeled()
        business_id ~ ChooseUniformly(possibilities[:business_id])
        neighbourhood_name ~ ChooseUniformly(possibilities[:neighbourhood_name])
    end

    @class Review begin
        rid ~ Unmodeled()
        business_id ~ ChooseUniformly(possibilities[:business_id])
        user_id ~ ChooseUniformly(possibilities[:user_id])
        rating ~ ChooseUniformly(possibilities[:rating])
        text ~ ChooseUniformly(possibilities[:text])
        year ~ ChooseUniformly(possibilities[:year])
        month ~ ChooseUniformly(possibilities[:month])
    end

    @class Tip begin
        tip_id ~ Unmodeled()
        business_id ~ ChooseUniformly(possibilities[:business_id])
        text ~ ChooseUniformly(possibilities[:text])
        user_id ~ ChooseUniformly(possibilities[:user_id])
        likes ~ ChooseUniformly(possibilities[:likes])
        year ~ ChooseUniformly(possibilities[:year])
        month ~ ChooseUniformly(possibilities[:month])
    end

    @class Obs begin
        business ~ Business
        category ~ Category
        user ~ User
        checkin ~ Checkin
        neighbourhood ~ Neighbourhood
        review ~ Review
        tip ~ Tip
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
    category_id category.id
    category_name category.category_name
    user_uid user.uid
    user_id user.user_id
    user_name user.name
    checkin_cid checkin.cid
    checkin_count checkin.count
    checkin_day checkin.day
    neighbourhood_id neighbourhood.id
    neighbourhood_name neighbourhood.neighbourhood_name
    review_rid review.rid
    review_rating review.rating
    review_text review.text
    review_year review.year
    review_month review.month
    tip_id tip.tip_id
    tip_text tip.text
    tip_likes tip.likes
    tip_year tip.year
    tip_month tip.month
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
