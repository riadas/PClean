using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("premises_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("premises_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "premise id"], Any[0, "premises type"], Any[0, "premise details"], Any[1, "product id"], Any[1, "product category"], Any[1, "product name"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "customer address"], Any[2, "customer login"], Any[2, "customer password"], Any[3, "mailshot id"], Any[3, "product category"], Any[3, "mailshot name"], Any[3, "mailshot start date"], Any[3, "mailshot end date"], Any[4, "customer id"], Any[4, "premise id"], Any[4, "date address from"], Any[4, "address type code"], Any[4, "date address to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status code"], Any[5, "shipping method code"], Any[5, "order placed datetime"], Any[5, "order delivered datetime"], Any[5, "order shipping charges"], Any[6, "mailshot id"], Any[6, "customer id"], Any[6, "outcome code"], Any[6, "mailshot customer date"], Any[7, "item id"], Any[7, "order item status code"], Any[7, "order id"], Any[7, "product id"], Any[7, "item status code"], Any[7, "item delivered datetime"], Any[7, "item order quantity"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "premise id"], Any[0, "premises type"], Any[0, "premise details"], Any[1, "product id"], Any[1, "product category"], Any[1, "product name"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "customer address"], Any[2, "customer login"], Any[2, "customer password"], Any[3, "mailshot id"], Any[3, "product category"], Any[3, "mailshot name"], Any[3, "mailshot start date"], Any[3, "mailshot end date"], Any[4, "customer id"], Any[4, "premise id"], Any[4, "date address from"], Any[4, "address type code"], Any[4, "date address to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status code"], Any[5, "shipping method code"], Any[5, "order placed datetime"], Any[5, "order delivered datetime"], Any[5, "order shipping charges"], Any[6, "mailshot id"], Any[6, "customer id"], Any[6, "outcome code"], Any[6, "mailshot customer date"], Any[7, "item id"], Any[7, "order item status code"], Any[7, "order id"], Any[7, "product id"], Any[7, "item status code"], Any[7, "item delivered datetime"], Any[7, "item order quantity"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "premise id"], Any[0, "premises type"], Any[0, "premise details"], Any[1, "product id"], Any[1, "product category"], Any[1, "product name"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "customer address"], Any[2, "customer login"], Any[2, "customer password"], Any[3, "mailshot id"], Any[3, "product category"], Any[3, "mailshot name"], Any[3, "mailshot start date"], Any[3, "mailshot end date"], Any[4, "customer id"], Any[4, "premise id"], Any[4, "date address from"], Any[4, "address type code"], Any[4, "date address to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status code"], Any[5, "shipping method code"], Any[5, "order placed datetime"], Any[5, "order delivered datetime"], Any[5, "order shipping charges"], Any[6, "mailshot id"], Any[6, "customer id"], Any[6, "outcome code"], Any[6, "mailshot customer date"], Any[7, "item id"], Any[7, "order item status code"], Any[7, "order id"], Any[7, "product id"], Any[7, "item status code"], Any[7, "item delivered datetime"], Any[7, "item order quantity"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "premise id"], Any[0, "premises type"], Any[0, "premise details"], Any[1, "product id"], Any[1, "product category"], Any[1, "product name"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "customer address"], Any[2, "customer login"], Any[2, "customer password"], Any[3, "mailshot id"], Any[3, "product category"], Any[3, "mailshot name"], Any[3, "mailshot start date"], Any[3, "mailshot end date"], Any[4, "customer id"], Any[4, "premise id"], Any[4, "date address from"], Any[4, "address type code"], Any[4, "date address to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status code"], Any[5, "shipping method code"], Any[5, "order placed datetime"], Any[5, "order delivered datetime"], Any[5, "order shipping charges"], Any[6, "mailshot id"], Any[6, "customer id"], Any[6, "outcome code"], Any[6, "mailshot customer date"], Any[7, "item id"], Any[7, "order item status code"], Any[7, "order id"], Any[7, "product id"], Any[7, "item status code"], Any[7, "item delivered datetime"], Any[7, "item order quantity"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "premise id"], Any[0, "premises type"], Any[0, "premise details"], Any[1, "product id"], Any[1, "product category"], Any[1, "product name"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "customer address"], Any[2, "customer login"], Any[2, "customer password"], Any[3, "mailshot id"], Any[3, "product category"], Any[3, "mailshot name"], Any[3, "mailshot start date"], Any[3, "mailshot end date"], Any[4, "customer id"], Any[4, "premise id"], Any[4, "date address from"], Any[4, "address type code"], Any[4, "date address to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status code"], Any[5, "shipping method code"], Any[5, "order placed datetime"], Any[5, "order delivered datetime"], Any[5, "order shipping charges"], Any[6, "mailshot id"], Any[6, "customer id"], Any[6, "outcome code"], Any[6, "mailshot customer date"], Any[7, "item id"], Any[7, "order item status code"], Any[7, "order id"], Any[7, "product id"], Any[7, "item status code"], Any[7, "item delivered datetime"], Any[7, "item order quantity"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[20, 7], Any[21, 1], Any[26, 7], Any[32, 15], Any[33, 7], Any[38, 25], Any[39, 4]])
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







PClean.@model CustomersCampaignsEcommerceModel begin
    @class Premises begin
        premises_type ~ ChooseUniformly(possibilities[:premises_type])
        premise_details ~ ChooseUniformly(possibilities[:premise_details])
    end

    @class Products begin
        product_category ~ ChooseUniformly(possibilities[:product_category])
        product_name ~ ChooseUniformly(possibilities[:product_name])
    end

    @class Customers begin
        payment_method ~ ChooseUniformly(possibilities[:payment_method])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
        customer_address ~ ChooseUniformly(possibilities[:customer_address])
        customer_login ~ ChooseUniformly(possibilities[:customer_login])
        customer_password ~ ChooseUniformly(possibilities[:customer_password])
    end

    @class Mailshot_campaigns begin
        product_category ~ ChooseUniformly(possibilities[:product_category])
        mailshot_name ~ ChooseUniformly(possibilities[:mailshot_name])
        mailshot_start_date ~ TimePrior(possibilities[:mailshot_start_date])
        mailshot_end_date ~ TimePrior(possibilities[:mailshot_end_date])
    end

    @class Customer_addresses begin
        customers ~ Customers
        premises ~ Premises
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        address_type_code ~ ChooseUniformly(possibilities[:address_type_code])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
    end

    @class Customer_orders begin
        customers ~ Customers
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
        shipping_method_code ~ ChooseUniformly(possibilities[:shipping_method_code])
        order_placed_datetime ~ TimePrior(possibilities[:order_placed_datetime])
        order_delivered_datetime ~ TimePrior(possibilities[:order_delivered_datetime])
        order_shipping_charges ~ ChooseUniformly(possibilities[:order_shipping_charges])
    end

    @class Mailshot_customers begin
        mailshot_campaigns ~ Mailshot_campaigns
        customers ~ Customers
        outcome_code ~ ChooseUniformly(possibilities[:outcome_code])
        mailshot_customer_date ~ TimePrior(possibilities[:mailshot_customer_date])
    end

    @class Order_items begin
        item_id ~ Unmodeled()
        order_item_status_code ~ ChooseUniformly(possibilities[:order_item_status_code])
        customer_orders ~ Customer_orders
        products ~ Products
        item_status_code ~ ChooseUniformly(possibilities[:item_status_code])
        item_delivered_datetime ~ TimePrior(possibilities[:item_delivered_datetime])
        item_order_quantity ~ ChooseUniformly(possibilities[:item_order_quantity])
    end

    @class Obs begin
        customer_addresses ~ Customer_addresses
        mailshot_customers ~ Mailshot_customers
        order_items ~ Order_items
    end
end

query = @query CustomersCampaignsEcommerceModel.Obs [
    premises_premise_id customer_addresses.premises.premise_id
    premises_type customer_addresses.premises.premises_type
    premises_premise_details customer_addresses.premises.premise_details
    products_product_id order_items.products.product_id
    products_product_category order_items.products.product_category
    products_product_name order_items.products.product_name
    customers_customer_id customer_addresses.customers.customer_id
    customers_payment_method customer_addresses.customers.payment_method
    customers_customer_name customer_addresses.customers.customer_name
    customers_customer_phone customer_addresses.customers.customer_phone
    customers_customer_email customer_addresses.customers.customer_email
    customers_customer_address customer_addresses.customers.customer_address
    customers_customer_login customer_addresses.customers.customer_login
    customers_customer_password customer_addresses.customers.customer_password
    mailshot_campaigns_mailshot_id mailshot_customers.mailshot_campaigns.mailshot_id
    mailshot_campaigns_product_category mailshot_customers.mailshot_campaigns.product_category
    mailshot_campaigns_mailshot_name mailshot_customers.mailshot_campaigns.mailshot_name
    mailshot_campaigns_mailshot_start_date mailshot_customers.mailshot_campaigns.mailshot_start_date
    mailshot_campaigns_mailshot_end_date mailshot_customers.mailshot_campaigns.mailshot_end_date
    customer_addresses_date_address_from customer_addresses.date_address_from
    customer_addresses_address_type_code customer_addresses.address_type_code
    customer_addresses_date_address_to customer_addresses.date_address_to
    customer_orders_order_id order_items.customer_orders.order_id
    customer_orders_order_status_code order_items.customer_orders.order_status_code
    customer_orders_shipping_method_code order_items.customer_orders.shipping_method_code
    customer_orders_order_placed_datetime order_items.customer_orders.order_placed_datetime
    customer_orders_order_delivered_datetime order_items.customer_orders.order_delivered_datetime
    customer_orders_order_shipping_charges order_items.customer_orders.order_shipping_charges
    mailshot_customers_outcome_code mailshot_customers.outcome_code
    mailshot_customers_mailshot_customer_date mailshot_customers.mailshot_customer_date
    order_items_item_id order_items.item_id
    order_items_order_item_status_code order_items.order_item_status_code
    order_items_item_status_code order_items.item_status_code
    order_items_item_delivered_datetime order_items.item_delivered_datetime
    order_items_item_order_quantity order_items.item_order_quantity
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
