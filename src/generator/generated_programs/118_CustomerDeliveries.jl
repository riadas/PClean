using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("products_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("products_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product name"], Any[0, "product price"], Any[0, "product description"], Any[1, "address id"], Any[1, "address details"], Any[1, "city"], Any[1, "zip postcode"], Any[1, "state province county"], Any[1, "country"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "date became customer"], Any[3, "regular order id"], Any[3, "distributer id"], Any[4, "regular order id"], Any[4, "product id"], Any[5, "actual order id"], Any[5, "order status code"], Any[5, "regular order id"], Any[5, "actual order date"], Any[6, "actual order id"], Any[6, "product id"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "address type"], Any[7, "date to"], Any[8, "route id"], Any[8, "route name"], Any[8, "other route details"], Any[9, "location code"], Any[9, "route id"], Any[9, "location address id"], Any[9, "location name"], Any[10, "truck id"], Any[10, "truck licence number"], Any[10, "truck details"], Any[11, "employee id"], Any[11, "employee address id"], Any[11, "employee name"], Any[11, "employee phone"], Any[12, "location code"], Any[12, "actual order id"], Any[12, "delivery status code"], Any[12, "driver employee id"], Any[12, "truck id"], Any[12, "delivery date"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product name"], Any[0, "product price"], Any[0, "product description"], Any[1, "address id"], Any[1, "address details"], Any[1, "city"], Any[1, "zip postcode"], Any[1, "state province county"], Any[1, "country"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "date became customer"], Any[3, "regular order id"], Any[3, "distributer id"], Any[4, "regular order id"], Any[4, "product id"], Any[5, "actual order id"], Any[5, "order status code"], Any[5, "regular order id"], Any[5, "actual order date"], Any[6, "actual order id"], Any[6, "product id"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "address type"], Any[7, "date to"], Any[8, "route id"], Any[8, "route name"], Any[8, "other route details"], Any[9, "location code"], Any[9, "route id"], Any[9, "location address id"], Any[9, "location name"], Any[10, "truck id"], Any[10, "truck licence number"], Any[10, "truck details"], Any[11, "employee id"], Any[11, "employee address id"], Any[11, "employee name"], Any[11, "employee phone"], Any[12, "location code"], Any[12, "actual order id"], Any[12, "delivery status code"], Any[12, "driver employee id"], Any[12, "truck id"], Any[12, "delivery date"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["distributer id", "regular order id", "product id", "regular order id", "actual order id", "product id", "address id", "customer id", "route id", "location address id", "employee address id", "driver employee id", "location code", "actual order id", "truck id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "product name"], Any[0, "product price"], Any[0, "product description"], Any[1, "address details"], Any[1, "city"], Any[1, "zip postcode"], Any[1, "state province county"], Any[1, "country"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "date became customer"], Any[5, "order status code"], Any[5, "actual order date"], Any[7, "date from"], Any[7, "address type"], Any[7, "date to"], Any[8, "route name"], Any[8, "other route details"], Any[9, "location name"], Any[10, "truck licence number"], Any[10, "truck details"], Any[11, "employee id"], Any[11, "employee name"], Any[11, "employee phone"], Any[12, "delivery status code"], Any[12, "delivery date"]]
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





PClean.@model CustomerDeliveriesModel begin
    @class Products begin
        product_id ~ Unmodeled()
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
        product_description ~ ChooseUniformly(possibilities[:product_description])
    end

    @class Addresses begin
        address_id ~ Unmodeled()
        address_details ~ ChooseUniformly(possibilities[:address_details])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Customers begin
        customer_id ~ Unmodeled()
        payment_method ~ ChooseUniformly(possibilities[:payment_method])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
    end

    @class Delivery_Routes begin
        route_id ~ Unmodeled()
        route_name ~ ChooseUniformly(possibilities[:route_name])
        other_route_details ~ ChooseUniformly(possibilities[:other_route_details])
    end

    @class Trucks begin
        truck_id ~ Unmodeled()
        truck_licence_number ~ ChooseUniformly(possibilities[:truck_licence_number])
        truck_details ~ ChooseUniformly(possibilities[:truck_details])
    end

    @class Obs begin
        products ~ Products
        addresses ~ Addresses
        customers ~ Customers
        delivery_Routes ~ Delivery_Routes
        trucks ~ Trucks
        regular_order_id ~ Unmodeled()
        actual_order_id ~ Unmodeled()
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
        actual_order_date ~ TimePrior(possibilities[:actual_order_date])
        date_from ~ TimePrior(possibilities[:date_from])
        address_type ~ ChooseUniformly(possibilities[:address_type])
        date_to ~ TimePrior(possibilities[:date_to])
        location_code ~ ChooseUniformly(possibilities[:location_code])
        location_name ~ ChooseUniformly(possibilities[:location_name])
        employee_id ~ Unmodeled()
        employee_name ~ ChooseUniformly(possibilities[:employee_name])
        employee_phone ~ ChooseUniformly(possibilities[:employee_phone])
        delivery_status_code ~ ChooseUniformly(possibilities[:delivery_status_code])
        delivery_date ~ TimePrior(possibilities[:delivery_date])
    end
end

query = @query CustomerDeliveriesModel.Obs [
    products_product_id products.product_id
    products_product_name products.product_name
    products_product_price products.product_price
    products_product_description products.product_description
    addresses_address_id addresses.address_id
    addresses_address_details addresses.address_details
    addresses_city addresses.city
    addresses_zip_postcode addresses.zip_postcode
    addresses_state_province_county addresses.state_province_county
    addresses_country addresses.country
    customers_customer_id customers.customer_id
    customers_payment_method customers.payment_method
    customers_customer_name customers.customer_name
    customers_customer_phone customers.customer_phone
    customers_customer_email customers.customer_email
    customers_date_became_customer customers.date_became_customer
    regular_orders_regular_order_id regular_order_id
    actual_orders_actual_order_id actual_order_id
    actual_orders_order_status_code order_status_code
    actual_orders_actual_order_date actual_order_date
    customer_addresses_date_from date_from
    customer_addresses_address_type address_type
    customer_addresses_date_to date_to
    delivery_routes_route_id delivery_Routes.route_id
    delivery_routes_route_name delivery_Routes.route_name
    delivery_routes_other_route_details delivery_Routes.other_route_details
    delivery_route_locations_location_code location_code
    delivery_route_locations_location_name location_name
    trucks_truck_id trucks.truck_id
    trucks_truck_licence_number trucks.truck_licence_number
    trucks_truck_details trucks.truck_details
    employees_employee_id employee_id
    employees_employee_name employee_name
    employees_employee_phone employee_phone
    order_deliveries_delivery_status_code delivery_status_code
    order_deliveries_delivery_date delivery_date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
