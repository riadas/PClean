using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("geographic_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("geographic_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "city name"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "city name"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"], Any[2, "city name"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "city name"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "city name"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"], Any[2, "city name"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        food_type ~ ChooseUniformly(possibilities[:food_type])
        city_name ~ ChooseUniformly(possibilities[:city_name])
        rating ~ ChooseUniformly(possibilities[:rating])
    end

    @class Location begin
        restaurant_id ~ Unmodeled()
        house_number ~ ChooseUniformly(possibilities[:house_number])
        street_name ~ ChooseUniformly(possibilities[:street_name])
        city_name ~ ChooseUniformly(possibilities[:city_name])
    end

    @class Obs begin
        geographic ~ Geographic
        restaurant ~ Restaurant
        location ~ Location
    end
end

query = @query RestaurantsModel.Obs [
    geographic_city_name geographic.city_name
    geographic_county geographic.county
    geographic_region geographic.region
    restaurant_id restaurant.id
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
