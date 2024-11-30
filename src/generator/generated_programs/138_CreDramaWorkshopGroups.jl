using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference payment methods_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference payment methods_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "payment method code"], Any[0, "payment method description"], Any[1, "service type code"], Any[1, "parent service type code"], Any[1, "service type description"], Any[2, "address id"], Any[2, "line 1"], Any[2, "line 2"], Any[2, "city town"], Any[2, "state county"], Any[2, "other details"], Any[3, "product id"], Any[3, "product name"], Any[3, "product price"], Any[3, "product description"], Any[3, "other product service details"], Any[4, "marketing region code"], Any[4, "marketing region name"], Any[4, "marketing region descriptrion"], Any[4, "other details"], Any[5, "client id"], Any[5, "address id"], Any[5, "customer email address"], Any[5, "customer name"], Any[5, "customer phone"], Any[5, "other details"], Any[6, "workshop group id"], Any[6, "address id"], Any[6, "currency code"], Any[6, "marketing region code"], Any[6, "store name"], Any[6, "store phone"], Any[6, "store email address"], Any[6, "other details"], Any[7, "performer id"], Any[7, "address id"], Any[7, "customer name"], Any[7, "customer phone"], Any[7, "customer email address"], Any[7, "other details"], Any[8, "customer id"], Any[8, "address id"], Any[8, "customer name"], Any[8, "customer phone"], Any[8, "customer email address"], Any[8, "other details"], Any[9, "store id"], Any[9, "address id"], Any[9, "marketing region code"], Any[9, "store name"], Any[9, "store phone"], Any[9, "store email address"], Any[9, "other details"], Any[10, "booking id"], Any[10, "customer id"], Any[10, "workshop group id"], Any[10, "status code"], Any[10, "store id"], Any[10, "order date"], Any[10, "planned delivery date"], Any[10, "actual delivery date"], Any[10, "other order details"], Any[11, "order id"], Any[11, "performer id"], Any[12, "order id"], Any[12, "customer id"], Any[12, "store id"], Any[12, "order date"], Any[12, "planned delivery date"], Any[12, "actual delivery date"], Any[12, "other order details"], Any[13, "order item id"], Any[13, "order id"], Any[13, "product id"], Any[13, "order quantity"], Any[13, "other item details"], Any[14, "invoice id"], Any[14, "order id"], Any[14, "payment method code"], Any[14, "product id"], Any[14, "order quantity"], Any[14, "other item details"], Any[14, "order item id"], Any[15, "service id"], Any[15, "service type code"], Any[15, "workshop group id"], Any[15, "product description"], Any[15, "product name"], Any[15, "product price"], Any[15, "other product service details"], Any[16, "order id"], Any[16, "product id"], Any[17, "invoice item id"], Any[17, "invoice id"], Any[17, "order id"], Any[17, "order item id"], Any[17, "product id"], Any[17, "order quantity"], Any[17, "other item details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "payment method code"], Any[0, "payment method description"], Any[1, "service type code"], Any[1, "parent service type code"], Any[1, "service type description"], Any[2, "address id"], Any[2, "line 1"], Any[2, "line 2"], Any[2, "city town"], Any[2, "state county"], Any[2, "other details"], Any[3, "product id"], Any[3, "product name"], Any[3, "product price"], Any[3, "product description"], Any[3, "other product service details"], Any[4, "marketing region code"], Any[4, "marketing region name"], Any[4, "marketing region descriptrion"], Any[4, "other details"], Any[5, "client id"], Any[5, "address id"], Any[5, "customer email address"], Any[5, "customer name"], Any[5, "customer phone"], Any[5, "other details"], Any[6, "workshop group id"], Any[6, "address id"], Any[6, "currency code"], Any[6, "marketing region code"], Any[6, "store name"], Any[6, "store phone"], Any[6, "store email address"], Any[6, "other details"], Any[7, "performer id"], Any[7, "address id"], Any[7, "customer name"], Any[7, "customer phone"], Any[7, "customer email address"], Any[7, "other details"], Any[8, "customer id"], Any[8, "address id"], Any[8, "customer name"], Any[8, "customer phone"], Any[8, "customer email address"], Any[8, "other details"], Any[9, "store id"], Any[9, "address id"], Any[9, "marketing region code"], Any[9, "store name"], Any[9, "store phone"], Any[9, "store email address"], Any[9, "other details"], Any[10, "booking id"], Any[10, "customer id"], Any[10, "workshop group id"], Any[10, "status code"], Any[10, "store id"], Any[10, "order date"], Any[10, "planned delivery date"], Any[10, "actual delivery date"], Any[10, "other order details"], Any[11, "order id"], Any[11, "performer id"], Any[12, "order id"], Any[12, "customer id"], Any[12, "store id"], Any[12, "order date"], Any[12, "planned delivery date"], Any[12, "actual delivery date"], Any[12, "other order details"], Any[13, "order item id"], Any[13, "order id"], Any[13, "product id"], Any[13, "order quantity"], Any[13, "other item details"], Any[14, "invoice id"], Any[14, "order id"], Any[14, "payment method code"], Any[14, "product id"], Any[14, "order quantity"], Any[14, "other item details"], Any[14, "order item id"], Any[15, "service id"], Any[15, "service type code"], Any[15, "workshop group id"], Any[15, "product description"], Any[15, "product name"], Any[15, "product price"], Any[15, "other product service details"], Any[16, "order id"], Any[16, "product id"], Any[17, "invoice item id"], Any[17, "invoice id"], Any[17, "order id"], Any[17, "order item id"], Any[17, "product id"], Any[17, "order quantity"], Any[17, "other item details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["address id", "address id", "address id", "address id", "marketing region code", "address id", "workshop group id", "customer id", "order id", "performer id", "store id", "customer id", "product id", "order id", "payment method code", "order id", "order id", "service type code", "workshop group id", "product id", "order id", "order id", "product id", "invoice id", "order item id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "payment method description"], Any[1, "parent service type code"], Any[1, "service type description"], Any[2, "line 1"], Any[2, "line 2"], Any[2, "city town"], Any[2, "state county"], Any[2, "other details"], Any[3, "product name"], Any[3, "product price"], Any[3, "product description"], Any[3, "other product service details"], Any[4, "marketing region name"], Any[4, "marketing region descriptrion"], Any[4, "other details"], Any[5, "client id"], Any[5, "customer email address"], Any[5, "customer name"], Any[5, "customer phone"], Any[5, "other details"], Any[6, "currency code"], Any[6, "store name"], Any[6, "store phone"], Any[6, "store email address"], Any[6, "other details"], Any[7, "customer name"], Any[7, "customer phone"], Any[7, "customer email address"], Any[7, "other details"], Any[8, "customer name"], Any[8, "customer phone"], Any[8, "customer email address"], Any[8, "other details"], Any[9, "store name"], Any[9, "store phone"], Any[9, "store email address"], Any[9, "other details"], Any[10, "booking id"], Any[10, "status code"], Any[10, "order date"], Any[10, "planned delivery date"], Any[10, "actual delivery date"], Any[10, "other order details"], Any[12, "order date"], Any[12, "planned delivery date"], Any[12, "actual delivery date"], Any[12, "other order details"], Any[13, "order quantity"], Any[13, "other item details"], Any[14, "order quantity"], Any[14, "other item details"], Any[15, "service id"], Any[15, "product description"], Any[15, "product name"], Any[15, "product price"], Any[15, "other product service details"], Any[17, "invoice item id"], Any[17, "order quantity"], Any[17, "other item details"]]
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





PClean.@model CreDramaWorkshopGroupsModel begin
    @class Reference_Payment_Methods begin
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        payment_method_description ~ ChooseUniformly(possibilities[:payment_method_description])
    end

    @class Reference_Service_Types begin
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
        parent_service_type_code ~ ChooseUniformly(possibilities[:parent_service_type_code])
        service_type_description ~ ChooseUniformly(possibilities[:service_type_description])
    end

    @class Addresses begin
        address_id ~ ChooseUniformly(possibilities[:address_id])
        line_1 ~ ChooseUniformly(possibilities[:line_1])
        line_2 ~ ChooseUniformly(possibilities[:line_2])
        city_town ~ ChooseUniformly(possibilities[:city_town])
        state_county ~ ChooseUniformly(possibilities[:state_county])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Products begin
        product_id ~ ChooseUniformly(possibilities[:product_id])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
        product_description ~ ChooseUniformly(possibilities[:product_description])
        other_product_service_details ~ ChooseUniformly(possibilities[:other_product_service_details])
    end

    @class Marketing_Regions begin
        marketing_region_code ~ ChooseUniformly(possibilities[:marketing_region_code])
        marketing_region_name ~ ChooseUniformly(possibilities[:marketing_region_name])
        marketing_region_descriptrion ~ ChooseUniformly(possibilities[:marketing_region_descriptrion])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        reference_Payment_Methods ~ Reference_Payment_Methods
        reference_Service_Types ~ Reference_Service_Types
        addresses ~ Addresses
        products ~ Products
        marketing_Regions ~ Marketing_Regions
        client_id ~ Unmodeled()
        customer_email_address ~ ChooseUniformly(possibilities[:customer_email_address])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        workshop_group_id ~ Unmodeled()
        currency_code ~ ChooseUniformly(possibilities[:currency_code])
        marketing_region_code ~ ChooseUniformly(possibilities[:marketing_region_code])
        store_name ~ ChooseUniformly(possibilities[:store_name])
        store_phone ~ ChooseUniformly(possibilities[:store_phone])
        store_email_address ~ ChooseUniformly(possibilities[:store_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        performer_id ~ Unmodeled()
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email_address ~ ChooseUniformly(possibilities[:customer_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email_address ~ ChooseUniformly(possibilities[:customer_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        store_id ~ ChooseUniformly(possibilities[:store_id])
        store_name ~ ChooseUniformly(possibilities[:store_name])
        store_phone ~ ChooseUniformly(possibilities[:store_phone])
        store_email_address ~ ChooseUniformly(possibilities[:store_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        booking_id ~ Unmodeled()
        status_code ~ ChooseUniformly(possibilities[:status_code])
        store_id ~ ChooseUniformly(possibilities[:store_id])
        order_date ~ TimePrior(possibilities[:order_date])
        planned_delivery_date ~ TimePrior(possibilities[:planned_delivery_date])
        actual_delivery_date ~ TimePrior(possibilities[:actual_delivery_date])
        other_order_details ~ ChooseUniformly(possibilities[:other_order_details])
        order_id ~ Unmodeled()
        order_date ~ TimePrior(possibilities[:order_date])
        planned_delivery_date ~ TimePrior(possibilities[:planned_delivery_date])
        actual_delivery_date ~ TimePrior(possibilities[:actual_delivery_date])
        other_order_details ~ ChooseUniformly(possibilities[:other_order_details])
        order_item_id ~ Unmodeled()
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
        other_item_details ~ ChooseUniformly(possibilities[:other_item_details])
        invoice_id ~ Unmodeled()
        product_id ~ ChooseUniformly(possibilities[:product_id])
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
        other_item_details ~ ChooseUniformly(possibilities[:other_item_details])
        order_item_id ~ ChooseUniformly(possibilities[:order_item_id])
        service_id ~ Unmodeled()
        product_description ~ ChooseUniformly(possibilities[:product_description])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
        other_product_service_details ~ ChooseUniformly(possibilities[:other_product_service_details])
        invoice_item_id ~ Unmodeled()
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
        other_item_details ~ ChooseUniformly(possibilities[:other_item_details])
    end
end

query = @query CreDramaWorkshopGroupsModel.Obs [
    reference_payment_methods_payment_method_code reference_Payment_Methods.payment_method_code
    reference_payment_methods_payment_method_description reference_Payment_Methods.payment_method_description
    reference_service_types_service_type_code reference_Service_Types.service_type_code
    reference_service_types_parent_service_type_code reference_Service_Types.parent_service_type_code
    reference_service_types_service_type_description reference_Service_Types.service_type_description
    addresses_address_id addresses.address_id
    addresses_line_1 addresses.line_1
    addresses_line_2 addresses.line_2
    addresses_city_town addresses.city_town
    addresses_state_county addresses.state_county
    addresses_other_details addresses.other_details
    products_product_id products.product_id
    products_product_name products.product_name
    products_product_price products.product_price
    products_product_description products.product_description
    products_other_product_service_details products.other_product_service_details
    marketing_regions_marketing_region_code marketing_Regions.marketing_region_code
    marketing_regions_marketing_region_name marketing_Regions.marketing_region_name
    marketing_regions_marketing_region_descriptrion marketing_Regions.marketing_region_descriptrion
    marketing_regions_other_details marketing_Regions.other_details
    clients_client_id client_id
    clients_customer_email_address customer_email_address
    clients_customer_name customer_name
    clients_customer_phone customer_phone
    clients_other_details other_details
    drama_workshop_groups_workshop_group_id workshop_group_id
    drama_workshop_groups_currency_code currency_code
    drama_workshop_groups_marketing_region_code marketing_region_code
    drama_workshop_groups_store_name store_name
    drama_workshop_groups_store_phone store_phone
    drama_workshop_groups_store_email_address store_email_address
    drama_workshop_groups_other_details other_details
    performers_performer_id performer_id
    performers_customer_name customer_name
    performers_customer_phone customer_phone
    performers_customer_email_address customer_email_address
    performers_other_details other_details
    customers_customer_id customer_id
    customers_customer_name customer_name
    customers_customer_phone customer_phone
    customers_customer_email_address customer_email_address
    customers_other_details other_details
    stores_store_id store_id
    stores_store_name store_name
    stores_store_phone store_phone
    stores_store_email_address store_email_address
    stores_other_details other_details
    bookings_booking_id booking_id
    bookings_status_code status_code
    bookings_store_id store_id
    bookings_order_date order_date
    bookings_planned_delivery_date planned_delivery_date
    bookings_actual_delivery_date actual_delivery_date
    bookings_other_order_details other_order_details
    customer_orders_order_id order_id
    customer_orders_order_date order_date
    customer_orders_planned_delivery_date planned_delivery_date
    customer_orders_actual_delivery_date actual_delivery_date
    customer_orders_other_order_details other_order_details
    order_items_order_item_id order_item_id
    order_items_order_quantity order_quantity
    order_items_other_item_details other_item_details
    invoices_invoice_id invoice_id
    invoices_product_id product_id
    invoices_order_quantity order_quantity
    invoices_other_item_details other_item_details
    invoices_order_item_id order_item_id
    services_service_id service_id
    services_product_description product_description
    services_product_name product_name
    services_product_price product_price
    services_other_product_service_details other_product_service_details
    invoice_items_invoice_item_id invoice_item_id
    invoice_items_order_quantity order_quantity
    invoice_items_other_item_details other_item_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
