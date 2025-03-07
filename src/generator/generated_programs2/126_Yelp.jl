using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("business_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("business_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "bid"], Any[0, "business id"], Any[0, "name"], Any[0, "full address"], Any[0, "city"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "review count"], Any[0, "is open"], Any[0, "rating"], Any[0, "state"], Any[1, "id"], Any[1, "business id"], Any[1, "category name"], Any[2, "uid"], Any[2, "user id"], Any[2, "name"], Any[3, "cid"], Any[3, "business id"], Any[3, "count"], Any[3, "day"], Any[4, "id"], Any[4, "business id"], Any[4, "neighbourhood name"], Any[5, "rid"], Any[5, "business id"], Any[5, "user id"], Any[5, "rating"], Any[5, "text"], Any[5, "year"], Any[5, "month"], Any[6, "tip id"], Any[6, "business id"], Any[6, "text"], Any[6, "user id"], Any[6, "likes"], Any[6, "year"], Any[6, "month"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 2], Any[19, 2], Any[23, 2], Any[27, 16], Any[26, 2], Any[35, 16], Any[33, 2]])
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







PClean.@model YelpModel begin
    @class Business begin
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
        business ~ Business
        category_name ~ ChooseUniformly(possibilities[:category_name])
    end

    @class User begin
        user_id ~ ChooseUniformly(possibilities[:user_id])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Checkin begin
        business ~ Business
        count ~ ChooseUniformly(possibilities[:count])
        day ~ ChooseUniformly(possibilities[:day])
    end

    @class Neighbourhood begin
        business ~ Business
        neighbourhood_name ~ ChooseUniformly(possibilities[:neighbourhood_name])
    end

    @class Review begin
        business ~ Business
        user ~ User
        rating ~ ChooseUniformly(possibilities[:rating])
        text ~ ChooseUniformly(possibilities[:text])
        year ~ ChooseUniformly(possibilities[:year])
        month ~ ChooseUniformly(possibilities[:month])
    end

    @class Tip begin
        business ~ Business
        text ~ ChooseUniformly(possibilities[:text])
        user ~ User
        likes ~ ChooseUniformly(possibilities[:likes])
        year ~ ChooseUniformly(possibilities[:year])
        month ~ ChooseUniformly(possibilities[:month])
    end

    @class Obs begin
        category ~ Category
        checkin ~ Checkin
        neighbourhood ~ Neighbourhood
        review ~ Review
        tip ~ Tip
    end
end

query = @query YelpModel.Obs [
    business_id category.business.business_id
    business_name category.business.name
    business_full_address category.business.full_address
    business_city category.business.city
    business_latitude category.business.latitude
    business_longitude category.business.longitude
    business_review_count category.business.review_count
    business_is_open category.business.is_open
    business_rating category.business.rating
    business_state category.business.state
    category_name category.category_name
    user_id review.user.user_id
    user_name review.user.name
    checkin_count checkin.count
    checkin_day checkin.day
    neighbourhood_name neighbourhood.neighbourhood_name
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
