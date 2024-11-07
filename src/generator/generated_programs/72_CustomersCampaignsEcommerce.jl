using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("premises_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("premises_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "premise id"], Any[0, "premises type"], Any[0, "premise details"], Any[1, "product id"], Any[1, "product category"], Any[1, "product name"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "customer address"], Any[2, "customer login"], Any[2, "customer password"], Any[3, "mailshot id"], Any[3, "product category"], Any[3, "mailshot name"], Any[3, "mailshot start date"], Any[3, "mailshot end date"], Any[4, "customer id"], Any[4, "premise id"], Any[4, "date address from"], Any[4, "address type code"], Any[4, "date address to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status code"], Any[5, "shipping method code"], Any[5, "order placed datetime"], Any[5, "order delivered datetime"], Any[5, "order shipping charges"], Any[6, "mailshot id"], Any[6, "customer id"], Any[6, "outcome code"], Any[6, "mailshot customer date"], Any[7, "item id"], Any[7, "order item status code"], Any[7, "order id"], Any[7, "product id"], Any[7, "item status code"], Any[7, "item delivered datetime"], Any[7, "item order quantity"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "premise id"], Any[0, "premises type"], Any[0, "premise details"], Any[1, "product id"], Any[1, "product category"], Any[1, "product name"], Any[2, "customer id"], Any[2, "payment method"], Any[2, "customer name"], Any[2, "customer phone"], Any[2, "customer email"], Any[2, "customer address"], Any[2, "customer login"], Any[2, "customer password"], Any[3, "mailshot id"], Any[3, "product category"], Any[3, "mailshot name"], Any[3, "mailshot start date"], Any[3, "mailshot end date"], Any[4, "customer id"], Any[4, "premise id"], Any[4, "date address from"], Any[4, "address type code"], Any[4, "date address to"], Any[5, "order id"], Any[5, "customer id"], Any[5, "order status code"], Any[5, "shipping method code"], Any[5, "order placed datetime"], Any[5, "order delivered datetime"], Any[5, "order shipping charges"], Any[6, "mailshot id"], Any[6, "customer id"], Any[6, "outcome code"], Any[6, "mailshot customer date"], Any[7, "item id"], Any[7, "order item status code"], Any[7, "order id"], Any[7, "product id"], Any[7, "item status code"], Any[7, "item delivered datetime"], Any[7, "item order quantity"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CustomersCampaignsEcommerceModel begin
    @class Premises begin
        premise_id ~ Unmodeled()
        premises_type ~ ChooseUniformly(possibilities[:premises_type])
        premise_details ~ ChooseUniformly(possibilities[:premise_details])
    end

    @class Products begin
        product_id ~ Unmodeled()
        product_category ~ ChooseUniformly(possibilities[:product_category])
        product_name ~ ChooseUniformly(possibilities[:product_name])
    end

    @class Customers begin
        customer_id ~ Unmodeled()
        payment_method ~ ChooseUniformly(possibilities[:payment_method])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
        customer_address ~ ChooseUniformly(possibilities[:customer_address])
        customer_login ~ ChooseUniformly(possibilities[:customer_login])
        customer_password ~ ChooseUniformly(possibilities[:customer_password])
    end

    @class Mailshot_Campaigns begin
        mailshot_id ~ Unmodeled()
        product_category ~ ChooseUniformly(possibilities[:product_category])
        mailshot_name ~ ChooseUniformly(possibilities[:mailshot_name])
        mailshot_start_date ~ TimePrior(possibilities[:mailshot_start_date])
        mailshot_end_date ~ TimePrior(possibilities[:mailshot_end_date])
    end

    @class Customer_Addresses begin
        customer_id ~ Unmodeled()
        premise_id ~ ChooseUniformly(possibilities[:premise_id])
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        address_type_code ~ ChooseUniformly(possibilities[:address_type_code])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
    end

    @class Customer_Orders begin
        order_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
        shipping_method_code ~ ChooseUniformly(possibilities[:shipping_method_code])
        order_placed_datetime ~ TimePrior(possibilities[:order_placed_datetime])
        order_delivered_datetime ~ TimePrior(possibilities[:order_delivered_datetime])
        order_shipping_charges ~ ChooseUniformly(possibilities[:order_shipping_charges])
    end

    @class Mailshot_Customers begin
        mailshot_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        outcome_code ~ ChooseUniformly(possibilities[:outcome_code])
        mailshot_customer_date ~ TimePrior(possibilities[:mailshot_customer_date])
    end

    @class Order_Items begin
        item_id ~ Unmodeled()
        order_item_status_code ~ ChooseUniformly(possibilities[:order_item_status_code])
        order_id ~ ChooseUniformly(possibilities[:order_id])
        product_id ~ ChooseUniformly(possibilities[:product_id])
        item_status_code ~ ChooseUniformly(possibilities[:item_status_code])
        item_delivered_datetime ~ TimePrior(possibilities[:item_delivered_datetime])
        item_order_quantity ~ ChooseUniformly(possibilities[:item_order_quantity])
    end

    @class Obs begin
        premises ~ Premises
        products ~ Products
        customers ~ Customers
        mailshot_Campaigns ~ Mailshot_Campaigns
        customer_Addresses ~ Customer_Addresses
        customer_Orders ~ Customer_Orders
        mailshot_Customers ~ Mailshot_Customers
        order_Items ~ Order_Items
    end
end

query = @query CustomersCampaignsEcommerceModel.Obs [
    premises_premise_id premises.premise_id
    premises_type premises.premises_type
    premises_premise_details premises.premise_details
    products_product_id products.product_id
    products_product_category products.product_category
    products_product_name products.product_name
    customers_customer_id customers.customer_id
    customers_payment_method customers.payment_method
    customers_customer_name customers.customer_name
    customers_customer_phone customers.customer_phone
    customers_customer_email customers.customer_email
    customers_customer_address customers.customer_address
    customers_customer_login customers.customer_login
    customers_customer_password customers.customer_password
    mailshot_campaigns_mailshot_id mailshot_Campaigns.mailshot_id
    mailshot_campaigns_product_category mailshot_Campaigns.product_category
    mailshot_campaigns_mailshot_name mailshot_Campaigns.mailshot_name
    mailshot_campaigns_mailshot_start_date mailshot_Campaigns.mailshot_start_date
    mailshot_campaigns_mailshot_end_date mailshot_Campaigns.mailshot_end_date
    customer_addresses_date_address_from customer_Addresses.date_address_from
    customer_addresses_address_type_code customer_Addresses.address_type_code
    customer_addresses_date_address_to customer_Addresses.date_address_to
    customer_orders_order_id customer_Orders.order_id
    customer_orders_order_status_code customer_Orders.order_status_code
    customer_orders_shipping_method_code customer_Orders.shipping_method_code
    customer_orders_order_placed_datetime customer_Orders.order_placed_datetime
    customer_orders_order_delivered_datetime customer_Orders.order_delivered_datetime
    customer_orders_order_shipping_charges customer_Orders.order_shipping_charges
    mailshot_customers_outcome_code mailshot_Customers.outcome_code
    mailshot_customers_mailshot_customer_date mailshot_Customers.mailshot_customer_date
    order_items_item_id order_Items.item_id
    order_items_order_item_status_code order_Items.order_item_status_code
    order_items_item_status_code order_Items.item_status_code
    order_items_item_delivered_datetime order_Items.item_delivered_datetime
    order_items_item_order_quantity order_Items.item_order_quantity
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
