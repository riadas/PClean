using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("geographic_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("geographic_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "city name"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "city name"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"], Any[2, "city name"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "city name"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "city name"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"], Any[2, "city name"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "city name"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "city name"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"], Any[2, "city name"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "city name"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "city name"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"], Any[2, "city name"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "city name"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "city name"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"], Any[2, "city name"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[12, 1]])
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







PClean.@model RestaurantsModel begin
    @class Geographic begin
        city_name ~ ChooseUniformly(possibilities[:city_name])
        county ~ ChooseUniformly(possibilities[:county])
        region ~ ChooseUniformly(possibilities[:region])
    end

    @class Restaurant begin
        name ~ ChooseUniformly(possibilities[:name])
        food_type ~ ChooseUniformly(possibilities[:food_type])
        geographic ~ Geographic
        rating ~ ChooseUniformly(possibilities[:rating])
    end

    @class Location begin
        house_number ~ ChooseUniformly(possibilities[:house_number])
        street_name ~ ChooseUniformly(possibilities[:street_name])
        geographic ~ Geographic
    end

    @class Obs begin
        restaurant ~ Restaurant
        location ~ Location
    end
end

query = @query RestaurantsModel.Obs [
    geographic_city_name restaurant.geographic.city_name
    geographic_county restaurant.geographic.county
    geographic_region restaurant.geographic.region
    restaurant_name restaurant.name
    restaurant_food_type restaurant.food_type
    restaurant_rating restaurant.rating
    location_restaurant_id location.restaurant_id
    location_house_number location.house_number
    location_street_name location.street_name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
