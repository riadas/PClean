using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("discount coupons_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("discount coupons_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["coupon id", "customer id", "customer id", "booking id", "product id", "booking id", "product id", "booking id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "date issued"], Any[0, "coupon amount"], Any[1, "good or bad customer"], Any[1, "first name"], Any[1, "last name"], Any[1, "gender"], Any[1, "date became customer"], Any[1, "date last hire"], Any[2, "booking status code"], Any[2, "returned damaged yes or no"], Any[2, "booking start date"], Any[2, "booking end date"], Any[2, "count hired"], Any[2, "amount payable"], Any[2, "amount of discount"], Any[2, "amount outstanding"], Any[2, "amount of refund"], Any[3, "product type code"], Any[3, "daily hire cost"], Any[3, "product name"], Any[3, "product description"], Any[4, "payment id"], Any[4, "payment type code"], Any[4, "amount paid in full yn"], Any[4, "payment date"], Any[4, "amount due"], Any[4, "amount paid"], Any[5, "returned yes or no"], Any[5, "returned late yes or no"], Any[5, "booked count"], Any[5, "booked amount"], Any[6, "status date"], Any[6, "available yes or no"]]
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





PClean.@model ProductsForHireModel begin
    @class Discount_Coupons begin
        coupon_id ~ Unmodeled()
        date_issued ~ TimePrior(possibilities[:date_issued])
        coupon_amount ~ ChooseUniformly(possibilities[:coupon_amount])
    end

    @class Products_For_Hire begin
        product_id ~ Unmodeled()
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        daily_hire_cost ~ ChooseUniformly(possibilities[:daily_hire_cost])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_description ~ ChooseUniformly(possibilities[:product_description])
    end

    @class Obs begin
        discount_Coupons ~ Discount_Coupons
        products_For_Hire ~ Products_For_Hire
        customer_id ~ Unmodeled()
        good_or_bad_customer ~ ChooseUniformly(possibilities[:good_or_bad_customer])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
        date_last_hire ~ TimePrior(possibilities[:date_last_hire])
        booking_id ~ Unmodeled()
        booking_status_code ~ ChooseUniformly(possibilities[:booking_status_code])
        returned_damaged_yes_or_no ~ ChooseUniformly(possibilities[:returned_damaged_yes_or_no])
        booking_start_date ~ TimePrior(possibilities[:booking_start_date])
        booking_end_date ~ TimePrior(possibilities[:booking_end_date])
        count_hired ~ ChooseUniformly(possibilities[:count_hired])
        amount_payable ~ ChooseUniformly(possibilities[:amount_payable])
        amount_of_discount ~ ChooseUniformly(possibilities[:amount_of_discount])
        amount_outstanding ~ ChooseUniformly(possibilities[:amount_outstanding])
        amount_of_refund ~ ChooseUniformly(possibilities[:amount_of_refund])
        payment_id ~ Unmodeled()
        payment_type_code ~ ChooseUniformly(possibilities[:payment_type_code])
        amount_paid_in_full_yn ~ ChooseUniformly(possibilities[:amount_paid_in_full_yn])
        payment_date ~ TimePrior(possibilities[:payment_date])
        amount_due ~ ChooseUniformly(possibilities[:amount_due])
        amount_paid ~ ChooseUniformly(possibilities[:amount_paid])
        returned_yes_or_no ~ ChooseUniformly(possibilities[:returned_yes_or_no])
        returned_late_yes_or_no ~ ChooseUniformly(possibilities[:returned_late_yes_or_no])
        booked_count ~ ChooseUniformly(possibilities[:booked_count])
        booked_amount ~ ChooseUniformly(possibilities[:booked_amount])
        status_date ~ TimePrior(possibilities[:status_date])
        available_yes_or_no ~ ChooseUniformly(possibilities[:available_yes_or_no])
    end
end

query = @query ProductsForHireModel.Obs [
    discount_coupons_coupon_id discount_Coupons.coupon_id
    discount_coupons_date_issued discount_Coupons.date_issued
    discount_coupons_coupon_amount discount_Coupons.coupon_amount
    customers_customer_id customer_id
    customers_good_or_bad_customer good_or_bad_customer
    customers_first_name first_name
    customers_last_name last_name
    customers_gender gender
    customers_date_became_customer date_became_customer
    customers_date_last_hire date_last_hire
    bookings_booking_id booking_id
    bookings_booking_status_code booking_status_code
    bookings_returned_damaged_yes_or_no returned_damaged_yes_or_no
    bookings_booking_start_date booking_start_date
    bookings_booking_end_date booking_end_date
    bookings_count_hired count_hired
    bookings_amount_payable amount_payable
    bookings_amount_of_discount amount_of_discount
    bookings_amount_outstanding amount_outstanding
    bookings_amount_of_refund amount_of_refund
    products_for_hire_product_id products_For_Hire.product_id
    products_for_hire_product_type_code products_For_Hire.product_type_code
    products_for_hire_daily_hire_cost products_For_Hire.daily_hire_cost
    products_for_hire_product_name products_For_Hire.product_name
    products_for_hire_product_description products_For_Hire.product_description
    payments_payment_id payment_id
    payments_payment_type_code payment_type_code
    payments_amount_paid_in_full_yn amount_paid_in_full_yn
    payments_payment_date payment_date
    payments_amount_due amount_due
    payments_amount_paid amount_paid
    products_booked_returned_yes_or_no returned_yes_or_no
    products_booked_returned_late_yes_or_no returned_late_yes_or_no
    products_booked_booked_count booked_count
    products_booked_booked_amount booked_amount
    view_product_availability_status_date status_date
    view_product_availability_available_yes_or_no available_yes_or_no
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
