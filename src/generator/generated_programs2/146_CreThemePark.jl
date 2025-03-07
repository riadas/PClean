using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("ref_hotel_star_ratings_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("ref_hotel_star_ratings_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "star rating code"], Any[0, "star rating description"], Any[1, "location id"], Any[1, "location name"], Any[1, "address"], Any[1, "other details"], Any[2, "attraction type code"], Any[2, "attraction type description"], Any[3, "tourist id"], Any[3, "tourist details"], Any[4, "feature id"], Any[4, "feature details"], Any[5, "hotel id"], Any[5, "star rating code"], Any[5, "pets allowed yn"], Any[5, "price range"], Any[5, "other hotel details"], Any[6, "tourist attraction id"], Any[6, "attraction type code"], Any[6, "location id"], Any[6, "how to get there"], Any[6, "name"], Any[6, "description"], Any[6, "opening hours"], Any[6, "other details"], Any[7, "market id"], Any[7, "market details"], Any[8, "shop id"], Any[8, "shop details"], Any[9, "museum id"], Any[9, "museum details"], Any[10, "royal family id"], Any[10, "royal family details"], Any[11, "theme park id"], Any[11, "theme park details"], Any[12, "visit id"], Any[12, "tourist attraction id"], Any[12, "tourist id"], Any[12, "visit date"], Any[12, "visit details"], Any[13, "photo id"], Any[13, "tourist attraction id"], Any[13, "name"], Any[13, "description"], Any[13, "filename"], Any[13, "other details"], Any[14, "staff id"], Any[14, "tourist attraction id"], Any[14, "name"], Any[14, "other details"], Any[15, "tourist attraction id"], Any[15, "feature id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[14, 1], Any[19, 7], Any[20, 3], Any[26, 18], Any[28, 18], Any[30, 18], Any[32, 18], Any[34, 18], Any[38, 9], Any[37, 18], Any[42, 18], Any[48, 18], Any[52, 11], Any[51, 18]])
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







PClean.@model CreThemeParkModel begin
    @class Ref_hotel_star_ratings begin
        star_rating_code ~ ChooseUniformly(possibilities[:star_rating_code])
        star_rating_description ~ ChooseUniformly(possibilities[:star_rating_description])
    end

    @class Locations begin
        location_name ~ ChooseUniformly(possibilities[:location_name])
        address ~ ChooseUniformly(possibilities[:address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Ref_attraction_types begin
        attraction_type_code ~ ChooseUniformly(possibilities[:attraction_type_code])
        attraction_type_description ~ ChooseUniformly(possibilities[:attraction_type_description])
    end

    @class Visitors begin
        tourist_details ~ ChooseUniformly(possibilities[:tourist_details])
    end

    @class Features begin
        feature_details ~ ChooseUniformly(possibilities[:feature_details])
    end

    @class Hotels begin
        ref_hotel_star_ratings ~ Ref_hotel_star_ratings
        pets_allowed_yn ~ ChooseUniformly(possibilities[:pets_allowed_yn])
        price_range ~ ChooseUniformly(possibilities[:price_range])
        other_hotel_details ~ ChooseUniformly(possibilities[:other_hotel_details])
    end

    @class Tourist_attractions begin
        ref_attraction_types ~ Ref_attraction_types
        locations ~ Locations
        how_to_get_there ~ ChooseUniformly(possibilities[:how_to_get_there])
        name ~ ChooseUniformly(possibilities[:name])
        description ~ ChooseUniformly(possibilities[:description])
        opening_hours ~ ChooseUniformly(possibilities[:opening_hours])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Street_markets begin
        market_details ~ ChooseUniformly(possibilities[:market_details])
    end

    @class Shops begin
        shop_details ~ ChooseUniformly(possibilities[:shop_details])
    end

    @class Museums begin
        museum_details ~ ChooseUniformly(possibilities[:museum_details])
    end

    @class Royal_family begin
        royal_family_details ~ ChooseUniformly(possibilities[:royal_family_details])
    end

    @class Theme_parks begin
        theme_park_details ~ ChooseUniformly(possibilities[:theme_park_details])
    end

    @class Visits begin
        tourist_attractions ~ Tourist_attractions
        visitors ~ Visitors
        visit_date ~ TimePrior(possibilities[:visit_date])
        visit_details ~ ChooseUniformly(possibilities[:visit_details])
    end

    @class Photos begin
        tourist_attractions ~ Tourist_attractions
        name ~ ChooseUniformly(possibilities[:name])
        description ~ ChooseUniformly(possibilities[:description])
        filename ~ ChooseUniformly(possibilities[:filename])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Staff begin
        tourist_attractions ~ Tourist_attractions
        name ~ ChooseUniformly(possibilities[:name])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Tourist_attraction_features begin
        features ~ Features
    end

    @class Obs begin
        hotels ~ Hotels
        street_markets ~ Street_markets
        shops ~ Shops
        museums ~ Museums
        royal_family ~ Royal_family
        theme_parks ~ Theme_parks
        visits ~ Visits
        photos ~ Photos
        staff ~ Staff
        tourist_attraction_features ~ Tourist_attraction_features
    end
end

query = @query CreThemeParkModel.Obs [
    ref_hotel_star_ratings_star_rating_code hotels.ref_hotel_star_ratings.star_rating_code
    ref_hotel_star_ratings_star_rating_description hotels.ref_hotel_star_ratings.star_rating_description
    locations_location_id street_markets.tourist_attractions.locations.location_id
    locations_location_name street_markets.tourist_attractions.locations.location_name
    locations_address street_markets.tourist_attractions.locations.address
    locations_other_details street_markets.tourist_attractions.locations.other_details
    ref_attraction_types_attraction_type_code street_markets.tourist_attractions.ref_attraction_types.attraction_type_code
    ref_attraction_types_attraction_type_description street_markets.tourist_attractions.ref_attraction_types.attraction_type_description
    visitors_tourist_id visits.visitors.tourist_id
    visitors_tourist_details visits.visitors.tourist_details
    features_feature_id tourist_attraction_features.features.feature_id
    features_feature_details tourist_attraction_features.features.feature_details
    hotels_hotel_id hotels.hotel_id
    hotels_pets_allowed_yn hotels.pets_allowed_yn
    hotels_price_range hotels.price_range
    hotels_other_hotel_details hotels.other_hotel_details
    tourist_attractions_tourist_attraction_id street_markets.tourist_attractions.tourist_attraction_id
    tourist_attractions_how_to_get_there street_markets.tourist_attractions.how_to_get_there
    tourist_attractions_name street_markets.tourist_attractions.name
    tourist_attractions_description street_markets.tourist_attractions.description
    tourist_attractions_opening_hours street_markets.tourist_attractions.opening_hours
    tourist_attractions_other_details street_markets.tourist_attractions.other_details
    street_markets_market_details street_markets.market_details
    shops_shop_details shops.shop_details
    museums_museum_details museums.museum_details
    royal_family_details royal_family.royal_family_details
    theme_parks_theme_park_details theme_parks.theme_park_details
    visits_visit_id visits.visit_id
    visits_visit_date visits.visit_date
    visits_visit_details visits.visit_details
    photos_photo_id photos.photo_id
    photos_name photos.name
    photos_description photos.description
    photos_filename photos.filename
    photos_other_details photos.other_details
    staff_id staff.staff_id
    staff_name staff.name
    staff_other_details staff.other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
