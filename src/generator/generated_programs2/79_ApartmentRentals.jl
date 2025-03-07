using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("apartment_buildings_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("apartment_buildings_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "building id"], Any[0, "building short name"], Any[0, "building full name"], Any[0, "building description"], Any[0, "building address"], Any[0, "building manager"], Any[0, "building phone"], Any[1, "apartment id"], Any[1, "building id"], Any[1, "apartment type code"], Any[1, "apartment number"], Any[1, "bathroom count"], Any[1, "bedroom count"], Any[1, "room count"], Any[2, "apartment id"], Any[2, "facility code"], Any[3, "guest id"], Any[3, "gender code"], Any[3, "guest first name"], Any[3, "guest last name"], Any[3, "date of birth"], Any[4, "apartment booking id"], Any[4, "apartment id"], Any[4, "guest id"], Any[4, "booking status code"], Any[4, "booking start date"], Any[4, "booking end date"], Any[5, "apartment id"], Any[5, "apartment booking id"], Any[5, "status date"], Any[5, "available yes or no"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "building id"], Any[0, "building short name"], Any[0, "building full name"], Any[0, "building description"], Any[0, "building address"], Any[0, "building manager"], Any[0, "building phone"], Any[1, "apartment id"], Any[1, "building id"], Any[1, "apartment type code"], Any[1, "apartment number"], Any[1, "bathroom count"], Any[1, "bedroom count"], Any[1, "room count"], Any[2, "apartment id"], Any[2, "facility code"], Any[3, "guest id"], Any[3, "gender code"], Any[3, "guest first name"], Any[3, "guest last name"], Any[3, "date of birth"], Any[4, "apartment booking id"], Any[4, "apartment id"], Any[4, "guest id"], Any[4, "booking status code"], Any[4, "booking start date"], Any[4, "booking end date"], Any[5, "apartment id"], Any[5, "apartment booking id"], Any[5, "status date"], Any[5, "available yes or no"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "building id"], Any[0, "building short name"], Any[0, "building full name"], Any[0, "building description"], Any[0, "building address"], Any[0, "building manager"], Any[0, "building phone"], Any[1, "apartment id"], Any[1, "building id"], Any[1, "apartment type code"], Any[1, "apartment number"], Any[1, "bathroom count"], Any[1, "bedroom count"], Any[1, "room count"], Any[2, "apartment id"], Any[2, "facility code"], Any[3, "guest id"], Any[3, "gender code"], Any[3, "guest first name"], Any[3, "guest last name"], Any[3, "date of birth"], Any[4, "apartment booking id"], Any[4, "apartment id"], Any[4, "guest id"], Any[4, "booking status code"], Any[4, "booking start date"], Any[4, "booking end date"], Any[5, "apartment id"], Any[5, "apartment booking id"], Any[5, "status date"], Any[5, "available yes or no"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "building id"], Any[0, "building short name"], Any[0, "building full name"], Any[0, "building description"], Any[0, "building address"], Any[0, "building manager"], Any[0, "building phone"], Any[1, "apartment id"], Any[1, "building id"], Any[1, "apartment type code"], Any[1, "apartment number"], Any[1, "bathroom count"], Any[1, "bedroom count"], Any[1, "room count"], Any[2, "apartment id"], Any[2, "facility code"], Any[3, "guest id"], Any[3, "gender code"], Any[3, "guest first name"], Any[3, "guest last name"], Any[3, "date of birth"], Any[4, "apartment booking id"], Any[4, "apartment id"], Any[4, "guest id"], Any[4, "booking status code"], Any[4, "booking start date"], Any[4, "booking end date"], Any[5, "apartment id"], Any[5, "apartment booking id"], Any[5, "status date"], Any[5, "available yes or no"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "building id"], Any[0, "building short name"], Any[0, "building full name"], Any[0, "building description"], Any[0, "building address"], Any[0, "building manager"], Any[0, "building phone"], Any[1, "apartment id"], Any[1, "building id"], Any[1, "apartment type code"], Any[1, "apartment number"], Any[1, "bathroom count"], Any[1, "bedroom count"], Any[1, "room count"], Any[2, "apartment id"], Any[2, "facility code"], Any[3, "guest id"], Any[3, "gender code"], Any[3, "guest first name"], Any[3, "guest last name"], Any[3, "date of birth"], Any[4, "apartment booking id"], Any[4, "apartment id"], Any[4, "guest id"], Any[4, "booking status code"], Any[4, "booking start date"], Any[4, "booking end date"], Any[5, "apartment id"], Any[5, "apartment booking id"], Any[5, "status date"], Any[5, "available yes or no"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 1], Any[15, 8], Any[24, 17], Any[23, 8], Any[29, 22], Any[28, 8]])
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







PClean.@model ApartmentRentalsModel begin
    @class Apartment_buildings begin
        building_short_name ~ ChooseUniformly(possibilities[:building_short_name])
        building_full_name ~ ChooseUniformly(possibilities[:building_full_name])
        building_description ~ ChooseUniformly(possibilities[:building_description])
        building_address ~ ChooseUniformly(possibilities[:building_address])
        building_manager ~ ChooseUniformly(possibilities[:building_manager])
        building_phone ~ ChooseUniformly(possibilities[:building_phone])
    end

    @class Apartments begin
        apartment_buildings ~ Apartment_buildings
        apartment_type_code ~ ChooseUniformly(possibilities[:apartment_type_code])
        apartment_number ~ ChooseUniformly(possibilities[:apartment_number])
        bathroom_count ~ ChooseUniformly(possibilities[:bathroom_count])
        bedroom_count ~ ChooseUniformly(possibilities[:bedroom_count])
        room_count ~ ChooseUniformly(possibilities[:room_count])
    end

    @class Apartment_facilities begin
        facility_code ~ ChooseUniformly(possibilities[:facility_code])
    end

    @class Guests begin
        gender_code ~ ChooseUniformly(possibilities[:gender_code])
        guest_first_name ~ ChooseUniformly(possibilities[:guest_first_name])
        guest_last_name ~ ChooseUniformly(possibilities[:guest_last_name])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
    end

    @class Apartment_bookings begin
        apartments ~ Apartments
        guests ~ Guests
        booking_status_code ~ ChooseUniformly(possibilities[:booking_status_code])
        booking_start_date ~ TimePrior(possibilities[:booking_start_date])
        booking_end_date ~ TimePrior(possibilities[:booking_end_date])
    end

    @class View_unit_status begin
        apartments ~ Apartments
        apartment_bookings ~ Apartment_bookings
        status_date ~ TimePrior(possibilities[:status_date])
        available_yes_or_no ~ ChooseUniformly(possibilities[:available_yes_or_no])
    end

    @class Obs begin
        apartment_facilities ~ Apartment_facilities
        view_unit_status ~ View_unit_status
    end
end

query = @query ApartmentRentalsModel.Obs [
    apartment_buildings_building_id apartment_facilities.apartments.apartment_buildings.building_id
    apartment_buildings_building_short_name apartment_facilities.apartments.apartment_buildings.building_short_name
    apartment_buildings_building_full_name apartment_facilities.apartments.apartment_buildings.building_full_name
    apartment_buildings_building_description apartment_facilities.apartments.apartment_buildings.building_description
    apartment_buildings_building_address apartment_facilities.apartments.apartment_buildings.building_address
    apartment_buildings_building_manager apartment_facilities.apartments.apartment_buildings.building_manager
    apartment_buildings_building_phone apartment_facilities.apartments.apartment_buildings.building_phone
    apartments_apartment_id apartment_facilities.apartments.apartment_id
    apartments_apartment_type_code apartment_facilities.apartments.apartment_type_code
    apartments_apartment_number apartment_facilities.apartments.apartment_number
    apartments_bathroom_count apartment_facilities.apartments.bathroom_count
    apartments_bedroom_count apartment_facilities.apartments.bedroom_count
    apartments_room_count apartment_facilities.apartments.room_count
    apartment_facilities_facility_code apartment_facilities.facility_code
    guests_guest_id view_unit_status.apartment_bookings.guests.guest_id
    guests_gender_code view_unit_status.apartment_bookings.guests.gender_code
    guests_guest_first_name view_unit_status.apartment_bookings.guests.guest_first_name
    guests_guest_last_name view_unit_status.apartment_bookings.guests.guest_last_name
    guests_date_of_birth view_unit_status.apartment_bookings.guests.date_of_birth
    apartment_bookings_apartment_booking_id view_unit_status.apartment_bookings.apartment_booking_id
    apartment_bookings_booking_status_code view_unit_status.apartment_bookings.booking_status_code
    apartment_bookings_booking_start_date view_unit_status.apartment_bookings.booking_start_date
    apartment_bookings_booking_end_date view_unit_status.apartment_bookings.booking_end_date
    view_unit_status_status_date view_unit_status.status_date
    view_unit_status_available_yes_or_no view_unit_status.available_yes_or_no
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
