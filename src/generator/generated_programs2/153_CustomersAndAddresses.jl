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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address content"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "product id"], Any[1, "product details"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "date became customer"], Any[2, "other customer details"], Any[3, "customer id"], Any[3, "address id"], Any[3, "date address from"], Any[3, "address type"], Any[3, "date address to"], Any[4, "customer id"], Any[4, "channel code"], Any[4, "active from date"], Any[4, "active to date"], Any[4, "contact number"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status"], Any[5, "order date"], Any[5, "order details"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address content"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "product id"], Any[1, "product details"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "date became customer"], Any[2, "other customer details"], Any[3, "customer id"], Any[3, "address id"], Any[3, "date address from"], Any[3, "address type"], Any[3, "date address to"], Any[4, "customer id"], Any[4, "channel code"], Any[4, "active from date"], Any[4, "active to date"], Any[4, "contact number"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status"], Any[5, "order date"], Any[5, "order details"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address content"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "product id"], Any[1, "product details"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "date became customer"], Any[2, "other customer details"], Any[3, "customer id"], Any[3, "address id"], Any[3, "date address from"], Any[3, "address type"], Any[3, "date address to"], Any[4, "customer id"], Any[4, "channel code"], Any[4, "active from date"], Any[4, "active to date"], Any[4, "contact number"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status"], Any[5, "order date"], Any[5, "order details"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address content"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "product id"], Any[1, "product details"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "date became customer"], Any[2, "other customer details"], Any[3, "customer id"], Any[3, "address id"], Any[3, "date address from"], Any[3, "address type"], Any[3, "date address to"], Any[4, "customer id"], Any[4, "channel code"], Any[4, "active from date"], Any[4, "active to date"], Any[4, "contact number"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status"], Any[5, "order date"], Any[5, "order details"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address content"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "product id"], Any[1, "product details"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "date became customer"], Any[2, "other customer details"], Any[3, "customer id"], Any[3, "address id"], Any[3, "date address from"], Any[3, "address type"], Any[3, "date address to"], Any[4, "customer id"], Any[4, "channel code"], Any[4, "active from date"], Any[4, "active to date"], Any[4, "contact number"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status"], Any[5, "order date"], Any[5, "order details"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[15, 10], Any[16, 1], Any[20, 10], Any[26, 10], Any[30, 25], Any[31, 8]])
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







PClean.@model CustomersAndAddressesModel begin
    @class Addresses begin
        address_content ~ ChooseUniformly(possibilities[:address_content])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
        other_address_details ~ ChooseUniformly(possibilities[:other_address_details])
    end

    @class Products begin
        product_details ~ ChooseUniformly(possibilities[:product_details])
    end

    @class Customers begin
        payment_method ~ ChooseUniformly(possibilities[:payment_method])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
        other_customer_details ~ ChooseUniformly(possibilities[:other_customer_details])
    end

    @class Customer_addresses begin
        customers ~ Customers
        addresses ~ Addresses
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        address_type ~ ChooseUniformly(possibilities[:address_type])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
    end

    @class Customer_contact_channels begin
        customers ~ Customers
        channel_code ~ ChooseUniformly(possibilities[:channel_code])
        active_from_date ~ TimePrior(possibilities[:active_from_date])
        active_to_date ~ TimePrior(possibilities[:active_to_date])
        contact_number ~ ChooseUniformly(possibilities[:contact_number])
    end

    @class Customer_orders begin
        customers ~ Customers
        order_status ~ ChooseUniformly(possibilities[:order_status])
        order_date ~ TimePrior(possibilities[:order_date])
        order_details ~ ChooseUniformly(possibilities[:order_details])
    end

    @class Order_items begin
        customer_orders ~ Customer_orders
        products ~ Products
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
    end

    @class Obs begin
        customer_addresses ~ Customer_addresses
        customer_contact_channels ~ Customer_contact_channels
        order_items ~ Order_items
    end
end

query = @query CustomersAndAddressesModel.Obs [
    addresses_address_id customer_addresses.addresses.address_id
    addresses_address_content customer_addresses.addresses.address_content
    addresses_city customer_addresses.addresses.city
    addresses_zip_postcode customer_addresses.addresses.zip_postcode
    addresses_state_province_county customer_addresses.addresses.state_province_county
    addresses_country customer_addresses.addresses.country
    addresses_other_address_details customer_addresses.addresses.other_address_details
    products_product_id order_items.products.product_id
    products_product_details order_items.products.product_details
    customers_customer_id customer_addresses.customers.customer_id
    customers_payment_method customer_addresses.customers.payment_method
    customers_customer_name customer_addresses.customers.customer_name
    customers_date_became_customer customer_addresses.customers.date_became_customer
    customers_other_customer_details customer_addresses.customers.other_customer_details
    customer_addresses_date_address_from customer_addresses.date_address_from
    customer_addresses_address_type customer_addresses.address_type
    customer_addresses_date_address_to customer_addresses.date_address_to
    customer_contact_channels_channel_code customer_contact_channels.channel_code
    customer_contact_channels_active_from_date customer_contact_channels.active_from_date
    customer_contact_channels_active_to_date customer_contact_channels.active_to_date
    customer_contact_channels_contact_number customer_contact_channels.contact_number
    customer_orders_order_id order_items.customer_orders.order_id
    customer_orders_order_status order_items.customer_orders.order_status
    customer_orders_order_date order_items.customer_orders.order_date
    customer_orders_order_details order_items.customer_orders.order_details
    order_items_order_quantity order_items.order_quantity
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
