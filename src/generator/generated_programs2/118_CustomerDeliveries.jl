using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("products_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("products_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product name"], Any[0, "product price"], Any[0, "product description"], Any[1, "address id"], Any[1, "address details"], Any[1, "city"], Any[1, "zip postcode"], Any[1, "state province county"], Any[1, "country"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "date became customer"], Any[3, "regular order id"], Any[3, "distributer id"], Any[4, "regular order id"], Any[4, "product id"], Any[5, "actual order id"], Any[5, "order status code"], Any[5, "regular order id"], Any[5, "actual order date"], Any[6, "actual order id"], Any[6, "product id"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "address type"], Any[7, "date to"], Any[8, "route id"], Any[8, "route name"], Any[8, "other route details"], Any[9, "location code"], Any[9, "route id"], Any[9, "location address id"], Any[9, "location name"], Any[10, "truck id"], Any[10, "truck licence number"], Any[10, "truck details"], Any[11, "employee id"], Any[11, "employee address id"], Any[11, "employee name"], Any[11, "employee phone"], Any[12, "location code"], Any[12, "actual order id"], Any[12, "delivery status code"], Any[12, "driver employee id"], Any[12, "truck id"], Any[12, "delivery date"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[18, 11], Any[19, 17], Any[20, 1], Any[23, 17], Any[25, 21], Any[26, 1], Any[28, 5], Any[27, 11], Any[36, 32], Any[37, 5], Any[43, 5], Any[49, 42], Any[46, 35], Any[47, 21], Any[50, 39]])
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







PClean.@model CustomerDeliveriesModel begin
    @class Products begin
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
        product_description ~ ChooseUniformly(possibilities[:product_description])
    end

    @class Addresses begin
        address_details ~ ChooseUniformly(possibilities[:address_details])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Customers begin
        payment_method ~ ChooseUniformly(possibilities[:payment_method])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
    end

    @class Regular_orders begin
        customers ~ Customers
    end

    @class Regular_order_products begin
        regular_orders ~ Regular_orders
        products ~ Products
    end

    @class Actual_orders begin
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
        regular_orders ~ Regular_orders
        actual_order_date ~ TimePrior(possibilities[:actual_order_date])
    end

    @class Actual_order_products begin
        actual_orders ~ Actual_orders
        products ~ Products
    end

    @class Customer_addresses begin
        customers ~ Customers
        addresses ~ Addresses
        date_from ~ TimePrior(possibilities[:date_from])
        address_type ~ ChooseUniformly(possibilities[:address_type])
        date_to ~ TimePrior(possibilities[:date_to])
    end

    @class Delivery_routes begin
        route_name ~ ChooseUniformly(possibilities[:route_name])
        other_route_details ~ ChooseUniformly(possibilities[:other_route_details])
    end

    @class Delivery_route_locations begin
        location_code ~ ChooseUniformly(possibilities[:location_code])
        delivery_routes ~ Delivery_routes
        addresses ~ Addresses
        location_name ~ ChooseUniformly(possibilities[:location_name])
    end

    @class Trucks begin
        truck_licence_number ~ ChooseUniformly(possibilities[:truck_licence_number])
        truck_details ~ ChooseUniformly(possibilities[:truck_details])
    end

    @class Employees begin
        addresses ~ Addresses
        employee_name ~ ChooseUniformly(possibilities[:employee_name])
        employee_phone ~ ChooseUniformly(possibilities[:employee_phone])
    end

    @class Order_deliveries begin
        delivery_route_locations ~ Delivery_route_locations
        actual_orders ~ Actual_orders
        delivery_status_code ~ ChooseUniformly(possibilities[:delivery_status_code])
        employees ~ Employees
        trucks ~ Trucks
        delivery_date ~ TimePrior(possibilities[:delivery_date])
    end

    @class Obs begin
        regular_order_products ~ Regular_order_products
        actual_order_products ~ Actual_order_products
        customer_addresses ~ Customer_addresses
        order_deliveries ~ Order_deliveries
    end
end

query = @query CustomerDeliveriesModel.Obs [
    products_product_id regular_order_products.products.product_id
    products_product_name regular_order_products.products.product_name
    products_product_price regular_order_products.products.product_price
    products_product_description regular_order_products.products.product_description
    addresses_address_id customer_addresses.addresses.address_id
    addresses_address_details customer_addresses.addresses.address_details
    addresses_city customer_addresses.addresses.city
    addresses_zip_postcode customer_addresses.addresses.zip_postcode
    addresses_state_province_county customer_addresses.addresses.state_province_county
    addresses_country customer_addresses.addresses.country
    customers_customer_id customer_addresses.customers.customer_id
    customers_payment_method customer_addresses.customers.payment_method
    customers_customer_name customer_addresses.customers.customer_name
    customers_customer_phone customer_addresses.customers.customer_phone
    customers_customer_email customer_addresses.customers.customer_email
    customers_date_became_customer customer_addresses.customers.date_became_customer
    regular_orders_regular_order_id regular_order_products.regular_orders.regular_order_id
    actual_orders_actual_order_id actual_order_products.actual_orders.actual_order_id
    actual_orders_order_status_code actual_order_products.actual_orders.order_status_code
    actual_orders_actual_order_date actual_order_products.actual_orders.actual_order_date
    customer_addresses_date_from customer_addresses.date_from
    customer_addresses_address_type customer_addresses.address_type
    customer_addresses_date_to customer_addresses.date_to
    delivery_routes_route_id order_deliveries.delivery_route_locations.delivery_routes.route_id
    delivery_routes_route_name order_deliveries.delivery_route_locations.delivery_routes.route_name
    delivery_routes_other_route_details order_deliveries.delivery_route_locations.delivery_routes.other_route_details
    delivery_route_locations_location_code order_deliveries.delivery_route_locations.location_code
    delivery_route_locations_location_name order_deliveries.delivery_route_locations.location_name
    trucks_truck_id order_deliveries.trucks.truck_id
    trucks_truck_licence_number order_deliveries.trucks.truck_licence_number
    trucks_truck_details order_deliveries.trucks.truck_details
    employees_employee_id order_deliveries.employees.employee_id
    employees_employee_name order_deliveries.employees.employee_name
    employees_employee_phone order_deliveries.employees.employee_phone
    order_deliveries_delivery_status_code order_deliveries.delivery_status_code
    order_deliveries_delivery_date order_deliveries.delivery_date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
