using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("discount coupons_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("discount coupons_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "coupon id"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "customer id"], Any[1, "coupon id"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking id"], Any[2, "customer id"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product id"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "booking id"], Any[4, "customer id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "booking id"], Any[5, "product id"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "product id"], Any[6, "booking id"], Any[6, "status date"], Any[6, "available yes or no"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "coupon id"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "customer id"], Any[1, "coupon id"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking id"], Any[2, "customer id"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product id"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "booking id"], Any[4, "customer id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "booking id"], Any[5, "product id"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "product id"], Any[6, "booking id"], Any[6, "status date"], Any[6, "available yes or no"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ProductsForHireModel begin
    @class Discount_Coupons begin
        coupon_id ~ Unmodeled()
        date_issued ~ TimePrior(possibilities[:date_issued])
        coupon_amount ~ ChooseUniformly(possibilities[:coupon_amount])
    end

    @class Customers begin
        customer_id ~ Unmodeled()
        coupon_id ~ ChooseUniformly(possibilities[:coupon_id])
        good_or_bad_customer ~ ChooseUniformly(possibilities[:good_or_bad_customer])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
        date_last_hire ~ TimePrior(possibilities[:date_last_hire])
    end

    @class Bookings begin
        booking_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
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

    @class Products_For_Hire begin
        product_id ~ Unmodeled()
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        daily_hire_cost ~ ChooseUniformly(possibilities[:daily_hire_cost])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_description ~ ChooseUniformly(possibilities[:product_description])
    end

    @class Payments begin
        payment_id ~ Unmodeled()
        booking_id ~ ChooseUniformly(possibilities[:booking_id])
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        payment_type_code ~ ChooseUniformly(possibilities[:payment_type_code])
        amount_paid_in_full_yn ~ ChooseUniformly(possibilities[:amount_paid_in_full_yn])
        payment_date ~ TimePrior(possibilities[:payment_date])
        amount_due ~ ChooseUniformly(possibilities[:amount_due])
        amount_paid ~ ChooseUniformly(possibilities[:amount_paid])
    end

    @class Products_Booked begin
        booking_id ~ Unmodeled()
        product_id ~ ChooseUniformly(possibilities[:product_id])
        returned_yes_or_no ~ ChooseUniformly(possibilities[:returned_yes_or_no])
        returned_late_yes_or_no ~ ChooseUniformly(possibilities[:returned_late_yes_or_no])
        booked_count ~ ChooseUniformly(possibilities[:booked_count])
        booked_amount ~ ChooseUniformly(possibilities[:booked_amount])
    end

    @class View_Product_Availability begin
        product_id ~ Unmodeled()
        booking_id ~ ChooseUniformly(possibilities[:booking_id])
        status_date ~ TimePrior(possibilities[:status_date])
        available_yes_or_no ~ ChooseUniformly(possibilities[:available_yes_or_no])
    end

    @class Obs begin
        discount_Coupons ~ Discount_Coupons
        customers ~ Customers
        bookings ~ Bookings
        products_For_Hire ~ Products_For_Hire
        payments ~ Payments
        products_Booked ~ Products_Booked
        view_Product_Availability ~ View_Product_Availability
    end
end

query = @query ProductsForHireModel.Obs [
    discount_coupons_coupon_id discount_Coupons.coupon_id
    discount_coupons_date_issued discount_Coupons.date_issued
    discount_coupons_coupon_amount discount_Coupons.coupon_amount
    customers_customer_id customers.customer_id
    customers_good_or_bad_customer customers.good_or_bad_customer
    customers_first_name customers.first_name
    customers_last_name customers.last_name
    customers_gender customers.gender
    customers_date_became_customer customers.date_became_customer
    customers_date_last_hire customers.date_last_hire
    bookings_booking_id bookings.booking_id
    bookings_booking_status_code bookings.booking_status_code
    bookings_returned_damaged_yes_or_no bookings.returned_damaged_yes_or_no
    bookings_booking_start_date bookings.booking_start_date
    bookings_booking_end_date bookings.booking_end_date
    bookings_count_hired bookings.count_hired
    bookings_amount_payable bookings.amount_payable
    bookings_amount_of_discount bookings.amount_of_discount
    bookings_amount_outstanding bookings.amount_outstanding
    bookings_amount_of_refund bookings.amount_of_refund
    products_for_hire_product_id products_For_Hire.product_id
    products_for_hire_product_type_code products_For_Hire.product_type_code
    products_for_hire_daily_hire_cost products_For_Hire.daily_hire_cost
    products_for_hire_product_name products_For_Hire.product_name
    products_for_hire_product_description products_For_Hire.product_description
    payments_payment_id payments.payment_id
    payments_payment_type_code payments.payment_type_code
    payments_amount_paid_in_full_yn payments.amount_paid_in_full_yn
    payments_payment_date payments.payment_date
    payments_amount_due payments.amount_due
    payments_amount_paid payments.amount_paid
    products_booked_returned_yes_or_no products_Booked.returned_yes_or_no
    products_booked_returned_late_yes_or_no products_Booked.returned_late_yes_or_no
    products_booked_booked_count products_Booked.booked_count
    products_booked_booked_amount products_Booked.booked_amount
    view_product_availability_status_date view_Product_Availability.status_date
    view_product_availability_available_yes_or_no view_Product_Availability.available_yes_or_no
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
