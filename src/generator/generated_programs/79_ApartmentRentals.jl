using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("apartment buildings_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("apartment buildings_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["building id", "apartment id", "guest id", "apartment id", "apartment booking id", "apartment id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "building short name"], Any[0, "building full name"], Any[0, "building description"], Any[0, "building address"], Any[0, "building manager"], Any[0, "building phone"], Any[1, "apartment type code"], Any[1, "apartment number"], Any[1, "bathroom count"], Any[1, "bedroom count"], Any[1, "room count"], Any[2, "facility code"], Any[3, "gender code"], Any[3, "guest first name"], Any[3, "guest last name"], Any[3, "date of birth"], Any[4, "booking status code"], Any[4, "booking start date"], Any[4, "booking end date"], Any[5, "status date"], Any[5, "available yes or no"]]
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





PClean.@model ApartmentRentalsModel begin
    @class Apartment_Buildings begin
        building_id ~ Unmodeled()
        building_short_name ~ ChooseUniformly(possibilities[:building_short_name])
        building_full_name ~ ChooseUniformly(possibilities[:building_full_name])
        building_description ~ ChooseUniformly(possibilities[:building_description])
        building_address ~ ChooseUniformly(possibilities[:building_address])
        building_manager ~ ChooseUniformly(possibilities[:building_manager])
        building_phone ~ ChooseUniformly(possibilities[:building_phone])
    end

    @class Guests begin
        guest_id ~ Unmodeled()
        gender_code ~ ChooseUniformly(possibilities[:gender_code])
        guest_first_name ~ ChooseUniformly(possibilities[:guest_first_name])
        guest_last_name ~ ChooseUniformly(possibilities[:guest_last_name])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
    end

    @class Obs begin
        apartment_Buildings ~ Apartment_Buildings
        guests ~ Guests
        apartment_id ~ Unmodeled()
        apartment_type_code ~ ChooseUniformly(possibilities[:apartment_type_code])
        apartment_number ~ ChooseUniformly(possibilities[:apartment_number])
        bathroom_count ~ ChooseUniformly(possibilities[:bathroom_count])
        bedroom_count ~ ChooseUniformly(possibilities[:bedroom_count])
        room_count ~ ChooseUniformly(possibilities[:room_count])
        facility_code ~ ChooseUniformly(possibilities[:facility_code])
        apartment_booking_id ~ Unmodeled()
        booking_status_code ~ ChooseUniformly(possibilities[:booking_status_code])
        booking_start_date ~ TimePrior(possibilities[:booking_start_date])
        booking_end_date ~ TimePrior(possibilities[:booking_end_date])
        status_date ~ TimePrior(possibilities[:status_date])
        available_yes_or_no ~ ChooseUniformly(possibilities[:available_yes_or_no])
    end
end

query = @query ApartmentRentalsModel.Obs [
    apartment_buildings_building_id apartment_Buildings.building_id
    apartment_buildings_building_short_name apartment_Buildings.building_short_name
    apartment_buildings_building_full_name apartment_Buildings.building_full_name
    apartment_buildings_building_description apartment_Buildings.building_description
    apartment_buildings_building_address apartment_Buildings.building_address
    apartment_buildings_building_manager apartment_Buildings.building_manager
    apartment_buildings_building_phone apartment_Buildings.building_phone
    apartments_apartment_id apartment_id
    apartments_apartment_type_code apartment_type_code
    apartments_apartment_number apartment_number
    apartments_bathroom_count bathroom_count
    apartments_bedroom_count bedroom_count
    apartments_room_count room_count
    apartment_facilities_facility_code facility_code
    guests_guest_id guests.guest_id
    guests_gender_code guests.gender_code
    guests_guest_first_name guests.guest_first_name
    guests_guest_last_name guests.guest_last_name
    guests_date_of_birth guests.date_of_birth
    apartment_bookings_apartment_booking_id apartment_booking_id
    apartment_bookings_booking_status_code booking_status_code
    apartment_bookings_booking_start_date booking_start_date
    apartment_bookings_booking_end_date booking_end_date
    view_unit_status_status_date status_date
    view_unit_status_available_yes_or_no available_yes_or_no
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
