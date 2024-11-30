using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "staff id"], Any[1, "staff address id"], Any[1, "nickname"], Any[1, "first name"], Any[1, "middle name"], Any[1, "last name"], Any[1, "date of birth"], Any[1, "date joined staff"], Any[1, "date left staff"], Any[2, "vehicle id"], Any[2, "vehicle details"], Any[3, "customer id"], Any[3, "customer address id"], Any[3, "customer status code"], Any[3, "date became customer"], Any[3, "date of birth"], Any[3, "first name"], Any[3, "last name"], Any[3, "amount outstanding"], Any[3, "email address"], Any[3, "phone number"], Any[3, "cell mobile phone number"], Any[4, "customer id"], Any[4, "datetime payment"], Any[4, "payment method code"], Any[4, "amount payment"], Any[5, "lesson id"], Any[5, "customer id"], Any[5, "lesson status code"], Any[5, "staff id"], Any[5, "vehicle id"], Any[5, "lesson date"], Any[5, "lesson time"], Any[5, "price"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "staff id"], Any[1, "staff address id"], Any[1, "nickname"], Any[1, "first name"], Any[1, "middle name"], Any[1, "last name"], Any[1, "date of birth"], Any[1, "date joined staff"], Any[1, "date left staff"], Any[2, "vehicle id"], Any[2, "vehicle details"], Any[3, "customer id"], Any[3, "customer address id"], Any[3, "customer status code"], Any[3, "date became customer"], Any[3, "date of birth"], Any[3, "first name"], Any[3, "last name"], Any[3, "amount outstanding"], Any[3, "email address"], Any[3, "phone number"], Any[3, "cell mobile phone number"], Any[4, "customer id"], Any[4, "datetime payment"], Any[4, "payment method code"], Any[4, "amount payment"], Any[5, "lesson id"], Any[5, "customer id"], Any[5, "lesson status code"], Any[5, "staff id"], Any[5, "vehicle id"], Any[5, "lesson date"], Any[5, "lesson time"], Any[5, "price"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["staff address id", "customer address id", "customer id", "customer id", "staff id", "vehicle id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "nickname"], Any[1, "first name"], Any[1, "middle name"], Any[1, "last name"], Any[1, "date of birth"], Any[1, "date joined staff"], Any[1, "date left staff"], Any[2, "vehicle details"], Any[3, "customer status code"], Any[3, "date became customer"], Any[3, "date of birth"], Any[3, "first name"], Any[3, "last name"], Any[3, "amount outstanding"], Any[3, "email address"], Any[3, "phone number"], Any[3, "cell mobile phone number"], Any[4, "datetime payment"], Any[4, "payment method code"], Any[4, "amount payment"], Any[5, "lesson id"], Any[5, "lesson status code"], Any[5, "lesson date"], Any[5, "lesson time"], Any[5, "price"]]
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





PClean.@model DrivingSchoolModel begin
    @class Addresses begin
        address_id ~ Unmodeled()
        line_1_number_building ~ ChooseUniformly(possibilities[:line_1_number_building])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Vehicles begin
        vehicle_id ~ Unmodeled()
        vehicle_details ~ ChooseUniformly(possibilities[:vehicle_details])
    end

    @class Obs begin
        addresses ~ Addresses
        vehicles ~ Vehicles
        staff_id ~ Unmodeled()
        nickname ~ ChooseUniformly(possibilities[:nickname])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        date_joined_staff ~ TimePrior(possibilities[:date_joined_staff])
        date_left_staff ~ TimePrior(possibilities[:date_left_staff])
        customer_id ~ Unmodeled()
        customer_status_code ~ ChooseUniformly(possibilities[:customer_status_code])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        amount_outstanding ~ ChooseUniformly(possibilities[:amount_outstanding])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        cell_mobile_phone_number ~ ChooseUniformly(possibilities[:cell_mobile_phone_number])
        datetime_payment ~ TimePrior(possibilities[:datetime_payment])
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        amount_payment ~ ChooseUniformly(possibilities[:amount_payment])
        lesson_id ~ Unmodeled()
        lesson_status_code ~ ChooseUniformly(possibilities[:lesson_status_code])
        lesson_date ~ TimePrior(possibilities[:lesson_date])
        lesson_time ~ ChooseUniformly(possibilities[:lesson_time])
        price ~ ChooseUniformly(possibilities[:price])
    end
end

query = @query DrivingSchoolModel.Obs [
    addresses_address_id addresses.address_id
    addresses_line_1_number_building addresses.line_1_number_building
    addresses_city addresses.city
    addresses_zip_postcode addresses.zip_postcode
    addresses_state_province_county addresses.state_province_county
    addresses_country addresses.country
    staff_id staff_id
    staff_nickname nickname
    staff_first_name first_name
    staff_middle_name middle_name
    staff_last_name last_name
    staff_date_of_birth date_of_birth
    date_joined_staff date_joined_staff
    date_left_staff date_left_staff
    vehicles_vehicle_id vehicles.vehicle_id
    vehicles_vehicle_details vehicles.vehicle_details
    customers_customer_id customer_id
    customers_customer_status_code customer_status_code
    customers_date_became_customer date_became_customer
    customers_date_of_birth date_of_birth
    customers_first_name first_name
    customers_last_name last_name
    customers_amount_outstanding amount_outstanding
    customers_email_address email_address
    customers_phone_number phone_number
    customers_cell_mobile_phone_number cell_mobile_phone_number
    customer_payments_datetime_payment datetime_payment
    customer_payments_payment_method_code payment_method_code
    customer_payments_amount_payment amount_payment
    lessons_lesson_id lesson_id
    lessons_lesson_status_code lesson_status_code
    lessons_lesson_date lesson_date
    lessons_lesson_time lesson_time
    lessons_price price
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
