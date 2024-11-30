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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product id"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "customer id"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "customer id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "customer id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product id"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "customer id"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "customer id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "customer id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order id"], Any[6, "product id"], Any[6, "order quantity"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["address id", "customer id", "customer id", "order id", "product id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "line 1 number building"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "product type code"], Any[1, "product name"], Any[1, "product price"], Any[2, "payment method code"], Any[2, "customer number"], Any[2, "customer name"], Any[2, "customer address"], Any[2, "customer phone"], Any[2, "customer email"], Any[3, "contact id"], Any[3, "gender"], Any[3, "first name"], Any[3, "last name"], Any[3, "contact phone"], Any[4, "date from"], Any[4, "date to"], Any[5, "order date"], Any[5, "order status code"], Any[6, "order item id"], Any[6, "order quantity"]]
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





PClean.@model CustomersAndProductsContactsModel begin
    @class Addresses begin
        address_id ~ Unmodeled()
        line_1_number_building ~ ChooseUniformly(possibilities[:line_1_number_building])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Products begin
        product_id ~ Unmodeled()
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
    end

    @class Customers begin
        customer_id ~ Unmodeled()
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        customer_number ~ ChooseUniformly(possibilities[:customer_number])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_address ~ ChooseUniformly(possibilities[:customer_address])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
    end

    @class Contacts begin
        contact_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        gender ~ ChooseUniformly(possibilities[:gender])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        contact_phone ~ ChooseUniformly(possibilities[:contact_phone])
    end

    @class Obs begin
        addresses ~ Addresses
        products ~ Products
        customers ~ Customers
        contacts ~ Contacts
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
        order_id ~ Unmodeled()
        order_date ~ TimePrior(possibilities[:order_date])
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
        order_item_id ~ Unmodeled()
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
    end
end

query = @query CustomersAndProductsContactsModel.Obs [
    addresses_address_id addresses.address_id
    addresses_line_1_number_building addresses.line_1_number_building
    addresses_city addresses.city
    addresses_zip_postcode addresses.zip_postcode
    addresses_state_province_county addresses.state_province_county
    addresses_country addresses.country
    products_product_id products.product_id
    products_product_type_code products.product_type_code
    products_product_name products.product_name
    products_product_price products.product_price
    customers_customer_id customers.customer_id
    customers_payment_method_code customers.payment_method_code
    customers_customer_number customers.customer_number
    customers_customer_name customers.customer_name
    customers_customer_address customers.customer_address
    customers_customer_phone customers.customer_phone
    customers_customer_email customers.customer_email
    contacts_contact_id contacts.contact_id
    contacts_customer_id contacts.customer_id
    contacts_gender contacts.gender
    contacts_first_name contacts.first_name
    contacts_last_name contacts.last_name
    contacts_contact_phone contacts.contact_phone
    customer_address_history_date_from date_from
    customer_address_history_date_to date_to
    customer_orders_order_id order_id
    customer_orders_order_date order_date
    customer_orders_order_status_code order_status_code
    order_items_order_item_id order_item_id
    order_items_order_quantity order_quantity
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
