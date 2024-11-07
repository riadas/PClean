using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address content"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "product id"], Any[1, "product details"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "date became customer"], Any[2, "other customer details"], Any[3, "customer id"], Any[3, "address id"], Any[3, "date address from"], Any[3, "address type"], Any[3, "date address to"], Any[4, "customer id"], Any[4, "channel code"], Any[4, "active from date"], Any[4, "active to date"], Any[4, "contact number"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status"], Any[5, "order date"], Any[5, "order details"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address content"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "product id"], Any[1, "product details"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "date became customer"], Any[2, "other customer details"], Any[3, "customer id"], Any[3, "address id"], Any[3, "date address from"], Any[3, "address type"], Any[3, "date address to"], Any[4, "customer id"], Any[4, "channel code"], Any[4, "active from date"], Any[4, "active to date"], Any[4, "contact number"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status"], Any[5, "order date"], Any[5, "order details"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CustomersAndAddressesModel begin
    @class Addresses begin
        address_id ~ Unmodeled()
        address_content ~ ChooseUniformly(possibilities[:address_content])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
        other_address_details ~ ChooseUniformly(possibilities[:other_address_details])
    end

    @class Products begin
        product_id ~ Unmodeled()
        product_details ~ ChooseUniformly(possibilities[:product_details])
    end

    @class Customers begin
        customer_id ~ Unmodeled()
        payment_method ~ ChooseUniformly(possibilities[:payment_method])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        date_became_customer ~ TimePrior(possibilities[:date_became_customer])
        other_customer_details ~ ChooseUniformly(possibilities[:other_customer_details])
    end

    @class Customer_Addresses begin
        customer_id ~ Unmodeled()
        address_id ~ ChooseUniformly(possibilities[:address_id])
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        address_type ~ ChooseUniformly(possibilities[:address_type])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
    end

    @class Customer_Contact_Channels begin
        customer_id ~ Unmodeled()
        channel_code ~ ChooseUniformly(possibilities[:channel_code])
        active_from_date ~ TimePrior(possibilities[:active_from_date])
        active_to_date ~ TimePrior(possibilities[:active_to_date])
        contact_number ~ ChooseUniformly(possibilities[:contact_number])
    end

    @class Customer_Orders begin
        order_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        order_status ~ ChooseUniformly(possibilities[:order_status])
        order_date ~ TimePrior(possibilities[:order_date])
        order_details ~ ChooseUniformly(possibilities[:order_details])
    end

    @class Order_Items begin
        order_id ~ Unmodeled()
        product_id ~ ChooseUniformly(possibilities[:product_id])
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
    end

    @class Obs begin
        addresses ~ Addresses
        products ~ Products
        customers ~ Customers
        customer_Addresses ~ Customer_Addresses
        customer_Contact_Channels ~ Customer_Contact_Channels
        customer_Orders ~ Customer_Orders
        order_Items ~ Order_Items
    end
end

query = @query CustomersAndAddressesModel.Obs [
    addresses_address_id addresses.address_id
    addresses_address_content addresses.address_content
    addresses_city addresses.city
    addresses_zip_postcode addresses.zip_postcode
    addresses_state_province_county addresses.state_province_county
    addresses_country addresses.country
    addresses_other_address_details addresses.other_address_details
    products_product_id products.product_id
    products_product_details products.product_details
    customers_customer_id customers.customer_id
    customers_payment_method customers.payment_method
    customers_customer_name customers.customer_name
    customers_date_became_customer customers.date_became_customer
    customers_other_customer_details customers.other_customer_details
    customer_addresses_date_address_from customer_Addresses.date_address_from
    customer_addresses_address_type customer_Addresses.address_type
    customer_addresses_date_address_to customer_Addresses.date_address_to
    customer_contact_channels_channel_code customer_Contact_Channels.channel_code
    customer_contact_channels_active_from_date customer_Contact_Channels.active_from_date
    customer_contact_channels_active_to_date customer_Contact_Channels.active_to_date
    customer_contact_channels_contact_number customer_Contact_Channels.contact_number
    customer_orders_order_id customer_Orders.order_id
    customer_orders_order_status customer_Orders.order_status
    customer_orders_order_date customer_Orders.order_date
    customer_orders_order_details customer_Orders.order_details
    order_items_order_quantity order_Items.order_quantity
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
