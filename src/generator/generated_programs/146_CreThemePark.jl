using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("ref hotel star ratings_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("ref hotel star ratings_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "star rating code"], Any[0, "star rating description"], Any[1, "location id"], Any[1, "location name"], Any[1, "address"], Any[1, "other details"], Any[2, "attraction type code"], Any[2, "attraction type description"], Any[3, "tourist id"], Any[3, "tourist details"], Any[4, "feature id"], Any[4, "feature details"], Any[5, "hotel id"], Any[5, "star rating code"], Any[5, "pets allowed yn"], Any[5, "price range"], Any[5, "other hotel details"], Any[6, "tourist attraction id"], Any[6, "attraction type code"], Any[6, "location id"], Any[6, "how to get there"], Any[6, "name"], Any[6, "description"], Any[6, "opening hours"], Any[6, "other details"], Any[7, "market id"], Any[7, "market details"], Any[8, "shop id"], Any[8, "shop details"], Any[9, "museum id"], Any[9, "museum details"], Any[10, "royal family id"], Any[10, "royal family details"], Any[11, "theme park id"], Any[11, "theme park details"], Any[12, "visit id"], Any[12, "tourist attraction id"], Any[12, "tourist id"], Any[12, "visit date"], Any[12, "visit details"], Any[13, "photo id"], Any[13, "tourist attraction id"], Any[13, "name"], Any[13, "description"], Any[13, "filename"], Any[13, "other details"], Any[14, "staff id"], Any[14, "tourist attraction id"], Any[14, "name"], Any[14, "other details"], Any[15, "tourist attraction id"], Any[15, "feature id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "star rating code"], Any[0, "star rating description"], Any[1, "location id"], Any[1, "location name"], Any[1, "address"], Any[1, "other details"], Any[2, "attraction type code"], Any[2, "attraction type description"], Any[3, "tourist id"], Any[3, "tourist details"], Any[4, "feature id"], Any[4, "feature details"], Any[5, "hotel id"], Any[5, "star rating code"], Any[5, "pets allowed yn"], Any[5, "price range"], Any[5, "other hotel details"], Any[6, "tourist attraction id"], Any[6, "attraction type code"], Any[6, "location id"], Any[6, "how to get there"], Any[6, "name"], Any[6, "description"], Any[6, "opening hours"], Any[6, "other details"], Any[7, "market id"], Any[7, "market details"], Any[8, "shop id"], Any[8, "shop details"], Any[9, "museum id"], Any[9, "museum details"], Any[10, "royal family id"], Any[10, "royal family details"], Any[11, "theme park id"], Any[11, "theme park details"], Any[12, "visit id"], Any[12, "tourist attraction id"], Any[12, "tourist id"], Any[12, "visit date"], Any[12, "visit details"], Any[13, "photo id"], Any[13, "tourist attraction id"], Any[13, "name"], Any[13, "description"], Any[13, "filename"], Any[13, "other details"], Any[14, "staff id"], Any[14, "tourist attraction id"], Any[14, "name"], Any[14, "other details"], Any[15, "tourist attraction id"], Any[15, "feature id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["star rating code", "attraction type code", "location id", "market id", "shop id", "museum id", "royal family id", "theme park id", "tourist id", "tourist attraction id", "tourist attraction id", "tourist attraction id", "feature id", "tourist attraction id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "star rating description"], Any[1, "location name"], Any[1, "address"], Any[1, "other details"], Any[2, "attraction type description"], Any[3, "tourist details"], Any[4, "feature details"], Any[5, "hotel id"], Any[5, "pets allowed yn"], Any[5, "price range"], Any[5, "other hotel details"], Any[6, "how to get there"], Any[6, "name"], Any[6, "description"], Any[6, "opening hours"], Any[6, "other details"], Any[7, "market details"], Any[8, "shop details"], Any[9, "museum details"], Any[10, "royal family details"], Any[11, "theme park details"], Any[12, "visit id"], Any[12, "visit date"], Any[12, "visit details"], Any[13, "photo id"], Any[13, "name"], Any[13, "description"], Any[13, "filename"], Any[13, "other details"], Any[14, "staff id"], Any[14, "name"], Any[14, "other details"]]
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





PClean.@model CreThemeParkModel begin
    @class Ref_Hotel_Star_Ratings begin
        star_rating_code ~ ChooseUniformly(possibilities[:star_rating_code])
        star_rating_description ~ ChooseUniformly(possibilities[:star_rating_description])
    end

    @class Locations begin
        location_id ~ Unmodeled()
        location_name ~ ChooseUniformly(possibilities[:location_name])
        address ~ ChooseUniformly(possibilities[:address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Ref_Attraction_Types begin
        attraction_type_code ~ ChooseUniformly(possibilities[:attraction_type_code])
        attraction_type_description ~ ChooseUniformly(possibilities[:attraction_type_description])
    end

    @class Visitors begin
        tourist_id ~ Unmodeled()
        tourist_details ~ ChooseUniformly(possibilities[:tourist_details])
    end

    @class Features begin
        feature_id ~ Unmodeled()
        feature_details ~ ChooseUniformly(possibilities[:feature_details])
    end

    @class Obs begin
        ref_Hotel_Star_Ratings ~ Ref_Hotel_Star_Ratings
        locations ~ Locations
        ref_Attraction_Types ~ Ref_Attraction_Types
        visitors ~ Visitors
        features ~ Features
        hotel_id ~ Unmodeled()
        pets_allowed_yn ~ ChooseUniformly(possibilities[:pets_allowed_yn])
        price_range ~ ChooseUniformly(possibilities[:price_range])
        other_hotel_details ~ ChooseUniformly(possibilities[:other_hotel_details])
        tourist_attraction_id ~ Unmodeled()
        how_to_get_there ~ ChooseUniformly(possibilities[:how_to_get_there])
        name ~ ChooseUniformly(possibilities[:name])
        description ~ ChooseUniformly(possibilities[:description])
        opening_hours ~ ChooseUniformly(possibilities[:opening_hours])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        market_details ~ ChooseUniformly(possibilities[:market_details])
        shop_details ~ ChooseUniformly(possibilities[:shop_details])
        museum_details ~ ChooseUniformly(possibilities[:museum_details])
        royal_family_details ~ ChooseUniformly(possibilities[:royal_family_details])
        theme_park_details ~ ChooseUniformly(possibilities[:theme_park_details])
        visit_id ~ Unmodeled()
        visit_date ~ TimePrior(possibilities[:visit_date])
        visit_details ~ ChooseUniformly(possibilities[:visit_details])
        photo_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        description ~ ChooseUniformly(possibilities[:description])
        filename ~ ChooseUniformly(possibilities[:filename])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        staff_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end
end

query = @query CreThemeParkModel.Obs [
    ref_hotel_star_ratings_star_rating_code ref_Hotel_Star_Ratings.star_rating_code
    ref_hotel_star_ratings_star_rating_description ref_Hotel_Star_Ratings.star_rating_description
    locations_location_id locations.location_id
    locations_location_name locations.location_name
    locations_address locations.address
    locations_other_details locations.other_details
    ref_attraction_types_attraction_type_code ref_Attraction_Types.attraction_type_code
    ref_attraction_types_attraction_type_description ref_Attraction_Types.attraction_type_description
    visitors_tourist_id visitors.tourist_id
    visitors_tourist_details visitors.tourist_details
    features_feature_id features.feature_id
    features_feature_details features.feature_details
    hotels_hotel_id hotel_id
    hotels_pets_allowed_yn pets_allowed_yn
    hotels_price_range price_range
    hotels_other_hotel_details other_hotel_details
    tourist_attractions_tourist_attraction_id tourist_attraction_id
    tourist_attractions_how_to_get_there how_to_get_there
    tourist_attractions_name name
    tourist_attractions_description description
    tourist_attractions_opening_hours opening_hours
    tourist_attractions_other_details other_details
    street_markets_market_details market_details
    shops_shop_details shop_details
    museums_museum_details museum_details
    royal_family_details royal_family_details
    theme_parks_theme_park_details theme_park_details
    visits_visit_id visit_id
    visits_visit_date visit_date
    visits_visit_details visit_details
    photos_photo_id photo_id
    photos_name name
    photos_description description
    photos_filename filename
    photos_other_details other_details
    staff_id staff_id
    staff_name name
    staff_other_details other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
