using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("geographic_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("geographic_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["city name", "city name"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "county"], Any[0, "region"], Any[1, "id"], Any[1, "name"], Any[1, "food type"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "house number"], Any[2, "street name"]]
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





PClean.@model RestaurantsModel begin
    @class Geographic begin
        city_name ~ ChooseUniformly(possibilities[:city_name])
        county ~ ChooseUniformly(possibilities[:county])
        region ~ ChooseUniformly(possibilities[:region])
    end

    @class Obs begin
        geographic ~ Geographic
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        food_type ~ ChooseUniformly(possibilities[:food_type])
        rating ~ ChooseUniformly(possibilities[:rating])
        restaurant_id ~ Unmodeled()
        house_number ~ ChooseUniformly(possibilities[:house_number])
        street_name ~ ChooseUniformly(possibilities[:street_name])
    end
end

query = @query RestaurantsModel.Obs [
    geographic_city_name geographic.city_name
    geographic_county geographic.county
    geographic_region geographic.region
    restaurant_id id
    restaurant_name name
    restaurant_food_type food_type
    restaurant_rating rating
    location_restaurant_id restaurant_id
    location_house_number house_number
    location_street_name street_name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
