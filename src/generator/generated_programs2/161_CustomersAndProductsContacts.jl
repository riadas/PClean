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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product id"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "customer id"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "customer id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "customer id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product id"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "customer id"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "customer id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "customer id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product id"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "customer id"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "customer id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "customer id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product id"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "customer id"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "customer id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "customer id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product id"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "customer id"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "customer id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "customer id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[25, 1], Any[24, 11], Any[29, 11], Any[33, 28], Any[34, 7]])
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







PClean.@model CustomersAndProductsContactsModel begin
    @class Addresses begin
        line_1_number_building ~ ChooseUniformly(possibilities[:line_1_number_building])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Products begin
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
    end

    @class Customers begin
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        customer_number ~ ChooseUniformly(possibilities[:customer_number])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_address ~ ChooseUniformly(possibilities[:customer_address])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
    end

    @class Contacts begin
        customer_id ~ Unmodeled()
        gender ~ ChooseUniformly(possibilities[:gender])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        contact_phone ~ ChooseUniformly(possibilities[:contact_phone])
    end

    @class Customer_address_history begin
        customers ~ Customers
        addresses ~ Addresses
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
    end

    @class Customer_orders begin
        customers ~ Customers
        order_date ~ TimePrior(possibilities[:order_date])
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
    end

    @class Order_items begin
        order_item_id ~ Unmodeled()
        customer_orders ~ Customer_orders
        products ~ Products
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
    end

    @class Obs begin
        contacts ~ Contacts
        customer_address_history ~ Customer_address_history
        order_items ~ Order_items
    end
end

query = @query CustomersAndProductsContactsModel.Obs [
    addresses_address_id customer_address_history.addresses.address_id
    addresses_line_1_number_building customer_address_history.addresses.line_1_number_building
    addresses_city customer_address_history.addresses.city
    addresses_zip_postcode customer_address_history.addresses.zip_postcode
    addresses_state_province_county customer_address_history.addresses.state_province_county
    addresses_country customer_address_history.addresses.country
    products_product_id order_items.products.product_id
    products_product_type_code order_items.products.product_type_code
    products_product_name order_items.products.product_name
    products_product_price order_items.products.product_price
    customers_customer_id customer_address_history.customers.customer_id
    customers_payment_method_code customer_address_history.customers.payment_method_code
    customers_customer_number customer_address_history.customers.customer_number
    customers_customer_name customer_address_history.customers.customer_name
    customers_customer_address customer_address_history.customers.customer_address
    customers_customer_phone customer_address_history.customers.customer_phone
    customers_customer_email customer_address_history.customers.customer_email
    contacts_contact_id contacts.contact_id
    contacts_customer_id contacts.customer_id
    contacts_gender contacts.gender
    contacts_first_name contacts.first_name
    contacts_last_name contacts.last_name
    contacts_contact_phone contacts.contact_phone
    customer_address_history_date_from customer_address_history.date_from
    customer_address_history_date_to customer_address_history.date_to
    customer_orders_order_id order_items.customer_orders.order_id
    customer_orders_order_date order_items.customer_orders.order_date
    customer_orders_order_status_code order_items.customer_orders.order_status_code
    order_items_order_item_id order_items.order_item_id
    order_items_order_quantity order_items.order_quantity
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
