using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("discount_coupons_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("discount_coupons_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "coupon id"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "customer id"], Any[1, "coupon id"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking id"], Any[2, "customer id"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product id"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "booking id"], Any[4, "customer id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "booking id"], Any[5, "product id"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "product id"], Any[6, "booking id"], Any[6, "status date"], Any[6, "available yes or no"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "coupon id"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "customer id"], Any[1, "coupon id"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking id"], Any[2, "customer id"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product id"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "booking id"], Any[4, "customer id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "booking id"], Any[5, "product id"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "product id"], Any[6, "booking id"], Any[6, "status date"], Any[6, "available yes or no"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "coupon id"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "customer id"], Any[1, "coupon id"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking id"], Any[2, "customer id"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product id"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "booking id"], Any[4, "customer id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "booking id"], Any[5, "product id"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "product id"], Any[6, "booking id"], Any[6, "status date"], Any[6, "available yes or no"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "coupon id"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "customer id"], Any[1, "coupon id"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking id"], Any[2, "customer id"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product id"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "booking id"], Any[4, "customer id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "booking id"], Any[5, "product id"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "product id"], Any[6, "booking id"], Any[6, "status date"], Any[6, "available yes or no"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "coupon id"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "customer id"], Any[1, "coupon id"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking id"], Any[2, "customer id"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product id"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "booking id"], Any[4, "customer id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "booking id"], Any[5, "product id"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "product id"], Any[6, "booking id"], Any[6, "status date"], Any[6, "available yes or no"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 1], Any[13, 4], Any[30, 4], Any[29, 12], Any[37, 23], Any[36, 12], Any[42, 23], Any[43, 12]])
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







PClean.@model ProductsForHireModel begin
    @class Discount_coupons begin
        date_issued ~ TimePrior(possibilities[:date_issued])
        coupon_amount ~ ChooseUniformly(possibilities[:coupon_amount])
    end

    @class Customers begin
        discount_coupons ~ Discount_coupons
        good_or_bad_customer ~ ChooseUniformly(possibilities[:good_or_bad_customer])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
        date_last_hire ~ TimePrior(possibilities[:date_last_hire])
    end

    @class Bookings begin
        customers ~ Customers
        booking_status_code ~ ChooseUniformly(possibilities[:booking_status_code])
        returned_damaged_yes_or_no ~ ChooseUniformly(possibilities[:returned_damaged_yes_or_no])
        booking_start_date ~ TimePrior(possibilities[:booking_start_date])
        booking_end_date ~ TimePrior(possibilities[:booking_end_date])
        count_hired ~ ChooseUniformly(possibilities[:count_hired])
        amount_payable ~ ChooseUniformly(possibilities[:amount_payable])
        amount_of_discount ~ ChooseUniformly(possibilities[:amount_of_discount])
        amount_outstanding ~ ChooseUniformly(possibilities[:amount_outstanding])
        amount_of_refund ~ ChooseUniformly(possibilities[:amount_of_refund])
    end

    @class Products_for_hire begin
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        daily_hire_cost ~ ChooseUniformly(possibilities[:daily_hire_cost])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_description ~ ChooseUniformly(possibilities[:product_description])
    end

    @class Payments begin
        bookings ~ Bookings
        customers ~ Customers
        payment_type_code ~ ChooseUniformly(possibilities[:payment_type_code])
        amount_paid_in_full_yn ~ ChooseUniformly(possibilities[:amount_paid_in_full_yn])
        payment_date ~ TimePrior(possibilities[:payment_date])
        amount_due ~ ChooseUniformly(possibilities[:amount_due])
        amount_paid ~ ChooseUniformly(possibilities[:amount_paid])
    end

    @class Products_booked begin
        products_for_hire ~ Products_for_hire
        returned_yes_or_no ~ ChooseUniformly(possibilities[:returned_yes_or_no])
        returned_late_yes_or_no ~ ChooseUniformly(possibilities[:returned_late_yes_or_no])
        booked_count ~ ChooseUniformly(possibilities[:booked_count])
        booked_amount ~ ChooseUniformly(possibilities[:booked_amount])
    end

    @class View_product_availability begin
        products_for_hire ~ Products_for_hire
        bookings ~ Bookings
        status_date ~ TimePrior(possibilities[:status_date])
        available_yes_or_no ~ ChooseUniformly(possibilities[:available_yes_or_no])
    end

    @class Obs begin
        payments ~ Payments
        products_booked ~ Products_booked
        view_product_availability ~ View_product_availability
    end
end

query = @query ProductsForHireModel.Obs [
    discount_coupons_coupon_id payments.bookings.customers.discount_coupons.coupon_id
    discount_coupons_date_issued payments.bookings.customers.discount_coupons.date_issued
    discount_coupons_coupon_amount payments.bookings.customers.discount_coupons.coupon_amount
    customers_customer_id payments.bookings.customers.customer_id
    customers_good_or_bad_customer payments.bookings.customers.good_or_bad_customer
    customers_first_name payments.bookings.customers.first_name
    customers_last_name payments.bookings.customers.last_name
    customers_gender payments.bookings.customers.gender
    customers_date_became_customer payments.bookings.customers.date_became_customer
    customers_date_last_hire payments.bookings.customers.date_last_hire
    bookings_booking_id payments.bookings.booking_id
    bookings_booking_status_code payments.bookings.booking_status_code
    bookings_returned_damaged_yes_or_no payments.bookings.returned_damaged_yes_or_no
    bookings_booking_start_date payments.bookings.booking_start_date
    bookings_booking_end_date payments.bookings.booking_end_date
    bookings_count_hired payments.bookings.count_hired
    bookings_amount_payable payments.bookings.amount_payable
    bookings_amount_of_discount payments.bookings.amount_of_discount
    bookings_amount_outstanding payments.bookings.amount_outstanding
    bookings_amount_of_refund payments.bookings.amount_of_refund
    products_for_hire_product_id products_booked.products_for_hire.product_id
    products_for_hire_product_type_code products_booked.products_for_hire.product_type_code
    products_for_hire_daily_hire_cost products_booked.products_for_hire.daily_hire_cost
    products_for_hire_product_name products_booked.products_for_hire.product_name
    products_for_hire_product_description products_booked.products_for_hire.product_description
    payments_payment_id payments.payment_id
    payments_payment_type_code payments.payment_type_code
    payments_amount_paid_in_full_yn payments.amount_paid_in_full_yn
    payments_payment_date payments.payment_date
    payments_amount_due payments.amount_due
    payments_amount_paid payments.amount_paid
    products_booked_returned_yes_or_no products_booked.returned_yes_or_no
    products_booked_returned_late_yes_or_no products_booked.returned_late_yes_or_no
    products_booked_booked_count products_booked.booked_count
    products_booked_booked_amount products_booked.booked_amount
    view_product_availability_status_date view_product_availability.status_date
    view_product_availability_available_yes_or_no view_product_availability.available_yes_or_no
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
