using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "staff id"], Any[1, "staff address id"], Any[1, "nickname"], Any[1, "first name"], Any[1, "middle name"], Any[1, "last name"], Any[1, "date of birth"], Any[1, "date joined staff"], Any[1, "date left staff"], Any[2, "vehicle id"], Any[2, "vehicle details"], Any[3, "customer id"], Any[3, "customer address id"], Any[3, "customer status code"], Any[3, "date became customer"], Any[3, "date of birth"], Any[3, "first name"], Any[3, "last name"], Any[3, "amount outstanding"], Any[3, "email address"], Any[3, "phone number"], Any[3, "cell mobile phone number"], Any[4, "customer id"], Any[4, "datetime payment"], Any[4, "payment method code"], Any[4, "amount payment"], Any[5, "lesson id"], Any[5, "customer id"], Any[5, "lesson status code"], Any[5, "staff id"], Any[5, "vehicle id"], Any[5, "lesson date"], Any[5, "lesson time"], Any[5, "price"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[8, 1], Any[19, 1], Any[29, 18], Any[34, 18], Any[36, 7], Any[37, 16]])
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







PClean.@model DrivingSchoolModel begin
    @class Addresses begin
        line_1_number_building ~ ChooseUniformly(possibilities[:line_1_number_building])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Staff begin
        addresses ~ Addresses
        nickname ~ ChooseUniformly(possibilities[:nickname])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        date_joined_staff ~ TimePrior(possibilities[:date_joined_staff])
        date_left_staff ~ TimePrior(possibilities[:date_left_staff])
    end

    @class Vehicles begin
        vehicle_details ~ ChooseUniformly(possibilities[:vehicle_details])
    end

    @class Customers begin
        addresses ~ Addresses
        customer_status_code ~ ChooseUniformly(possibilities[:customer_status_code])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        amount_outstanding ~ ChooseUniformly(possibilities[:amount_outstanding])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        cell_mobile_phone_number ~ ChooseUniformly(possibilities[:cell_mobile_phone_number])
    end

    @class Customer_payments begin
        datetime_payment ~ TimePrior(possibilities[:datetime_payment])
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        amount_payment ~ ChooseUniformly(possibilities[:amount_payment])
    end

    @class Lessons begin
        customers ~ Customers
        lesson_status_code ~ ChooseUniformly(possibilities[:lesson_status_code])
        staff ~ Staff
        vehicles ~ Vehicles
        lesson_date ~ TimePrior(possibilities[:lesson_date])
        lesson_time ~ ChooseUniformly(possibilities[:lesson_time])
        price ~ ChooseUniformly(possibilities[:price])
    end

    @class Obs begin
        customer_payments ~ Customer_payments
        lessons ~ Lessons
    end
end

query = @query DrivingSchoolModel.Obs [
    addresses_address_id lessons.customers.addresses.address_id
    addresses_line_1_number_building lessons.customers.addresses.line_1_number_building
    addresses_city lessons.customers.addresses.city
    addresses_zip_postcode lessons.customers.addresses.zip_postcode
    addresses_state_province_county lessons.customers.addresses.state_province_county
    addresses_country lessons.customers.addresses.country
    staff_id lessons.staff.staff_id
    staff_nickname lessons.staff.nickname
    staff_first_name lessons.staff.first_name
    staff_middle_name lessons.staff.middle_name
    staff_last_name lessons.staff.last_name
    staff_date_of_birth lessons.staff.date_of_birth
    date_joined_staff lessons.staff.date_joined_staff
    date_left_staff lessons.staff.date_left_staff
    vehicles_vehicle_id lessons.vehicles.vehicle_id
    vehicles_vehicle_details lessons.vehicles.vehicle_details
    customers_customer_id customer_payments.customers.customer_id
    customers_customer_status_code customer_payments.customers.customer_status_code
    customers_date_became_customer customer_payments.customers.date_became_customer
    customers_date_of_birth customer_payments.customers.date_of_birth
    customers_first_name customer_payments.customers.first_name
    customers_last_name customer_payments.customers.last_name
    customers_amount_outstanding customer_payments.customers.amount_outstanding
    customers_email_address customer_payments.customers.email_address
    customers_phone_number customer_payments.customers.phone_number
    customers_cell_mobile_phone_number customer_payments.customers.cell_mobile_phone_number
    customer_payments_datetime_payment customer_payments.datetime_payment
    customer_payments_payment_method_code customer_payments.payment_method_code
    customer_payments_amount_payment customer_payments.amount_payment
    lessons_lesson_id lessons.lesson_id
    lessons_lesson_status_code lessons.lesson_status_code
    lessons_lesson_date lessons.lesson_date
    lessons_lesson_time lessons.lesson_time
    lessons_price lessons.price
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
