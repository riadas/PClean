using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference_payment_methods_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference_payment_methods_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "payment method code"], Any[0, "payment method description"], Any[1, "service type code"], Any[1, "parent service type code"], Any[1, "service type description"], Any[2, "address id"], Any[2, "line 1"], Any[2, "line 2"], Any[2, "city town"], Any[2, "state county"], Any[2, "other details"], Any[3, "product id"], Any[3, "product name"], Any[3, "product price"], Any[3, "product description"], Any[3, "other product service details"], Any[4, "marketing region code"], Any[4, "marketing region name"], Any[4, "marketing region descriptrion"], Any[4, "other details"], Any[5, "client id"], Any[5, "address id"], Any[5, "customer email address"], Any[5, "customer name"], Any[5, "customer phone"], Any[5, "other details"], Any[6, "workshop group id"], Any[6, "address id"], Any[6, "currency code"], Any[6, "marketing region code"], Any[6, "store name"], Any[6, "store phone"], Any[6, "store email address"], Any[6, "other details"], Any[7, "performer id"], Any[7, "address id"], Any[7, "customer name"], Any[7, "customer phone"], Any[7, "customer email address"], Any[7, "other details"], Any[8, "customer id"], Any[8, "address id"], Any[8, "customer name"], Any[8, "customer phone"], Any[8, "customer email address"], Any[8, "other details"], Any[9, "store id"], Any[9, "address id"], Any[9, "marketing region code"], Any[9, "store name"], Any[9, "store phone"], Any[9, "store email address"], Any[9, "other details"], Any[10, "booking id"], Any[10, "customer id"], Any[10, "workshop group id"], Any[10, "status code"], Any[10, "store id"], Any[10, "order date"], Any[10, "planned delivery date"], Any[10, "actual delivery date"], Any[10, "other order details"], Any[11, "order id"], Any[11, "performer id"], Any[12, "order id"], Any[12, "customer id"], Any[12, "store id"], Any[12, "order date"], Any[12, "planned delivery date"], Any[12, "actual delivery date"], Any[12, "other order details"], Any[13, "order item id"], Any[13, "order id"], Any[13, "product id"], Any[13, "order quantity"], Any[13, "other item details"], Any[14, "invoice id"], Any[14, "order id"], Any[14, "payment method code"], Any[14, "product id"], Any[14, "order quantity"], Any[14, "other item details"], Any[14, "order item id"], Any[15, "service id"], Any[15, "service type code"], Any[15, "workshop group id"], Any[15, "product description"], Any[15, "product name"], Any[15, "product price"], Any[15, "other product service details"], Any[16, "order id"], Any[16, "product id"], Any[17, "invoice item id"], Any[17, "invoice id"], Any[17, "order id"], Any[17, "order item id"], Any[17, "product id"], Any[17, "order quantity"], Any[17, "other item details"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[22, 6], Any[28, 6], Any[36, 6], Any[42, 6], Any[49, 17], Any[48, 6], Any[56, 27], Any[55, 21], Any[63, 54], Any[64, 35], Any[67, 47], Any[66, 41], Any[74, 12], Any[73, 65], Any[79, 1], Any[78, 54], Any[78, 65], Any[85, 3], Any[86, 27], Any[92, 84], Any[91, 54], Any[95, 91], Any[97, 92], Any[94, 77], Any[96, 72]])
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







PClean.@model CreDramaWorkshopGroupsModel begin
    @class Reference_payment_methods begin
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        payment_method_description ~ ChooseUniformly(possibilities[:payment_method_description])
    end

    @class Reference_service_types begin
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

    @class Marketing_regions begin
        marketing_region_code ~ ChooseUniformly(possibilities[:marketing_region_code])
        marketing_region_name ~ ChooseUniformly(possibilities[:marketing_region_name])
        marketing_region_descriptrion ~ ChooseUniformly(possibilities[:marketing_region_descriptrion])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Clients begin
        addresses ~ Addresses
        customer_email_address ~ ChooseUniformly(possibilities[:customer_email_address])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Drama_workshop_groups begin
        addresses ~ Addresses
        currency_code ~ ChooseUniformly(possibilities[:currency_code])
        marketing_region_code ~ ChooseUniformly(possibilities[:marketing_region_code])
        store_name ~ ChooseUniformly(possibilities[:store_name])
        store_phone ~ ChooseUniformly(possibilities[:store_phone])
        store_email_address ~ ChooseUniformly(possibilities[:store_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Performers begin
        addresses ~ Addresses
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email_address ~ ChooseUniformly(possibilities[:customer_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Customers begin
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        addresses ~ Addresses
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email_address ~ ChooseUniformly(possibilities[:customer_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Stores begin
        store_id ~ ChooseUniformly(possibilities[:store_id])
        addresses ~ Addresses
        marketing_regions ~ Marketing_regions
        store_name ~ ChooseUniformly(possibilities[:store_name])
        store_phone ~ ChooseUniformly(possibilities[:store_phone])
        store_email_address ~ ChooseUniformly(possibilities[:store_email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Bookings begin
        clients ~ Clients
        drama_workshop_groups ~ Drama_workshop_groups
        status_code ~ ChooseUniformly(possibilities[:status_code])
        store_id ~ ChooseUniformly(possibilities[:store_id])
        order_date ~ TimePrior(possibilities[:order_date])
        planned_delivery_date ~ TimePrior(possibilities[:planned_delivery_date])
        actual_delivery_date ~ TimePrior(possibilities[:actual_delivery_date])
        other_order_details ~ ChooseUniformly(possibilities[:other_order_details])
    end

    @class Performers_in_bookings begin
        performers ~ Performers
    end

    @class Customer_orders begin
        customers ~ Customers
        stores ~ Stores
        order_date ~ TimePrior(possibilities[:order_date])
        planned_delivery_date ~ TimePrior(possibilities[:planned_delivery_date])
        actual_delivery_date ~ TimePrior(possibilities[:actual_delivery_date])
        other_order_details ~ ChooseUniformly(possibilities[:other_order_details])
    end

    @class Order_items begin
        customer_orders ~ Customer_orders
        products ~ Products
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
        other_item_details ~ ChooseUniformly(possibilities[:other_item_details])
    end

    @class Invoices begin
        bookings ~ Bookings
        reference_payment_methods ~ Reference_payment_methods
        product_id ~ ChooseUniformly(possibilities[:product_id])
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
        other_item_details ~ ChooseUniformly(possibilities[:other_item_details])
        order_item_id ~ ChooseUniformly(possibilities[:order_item_id])
    end

    @class Services begin
        reference_service_types ~ Reference_service_types
        drama_workshop_groups ~ Drama_workshop_groups
        product_description ~ ChooseUniformly(possibilities[:product_description])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
        other_product_service_details ~ ChooseUniformly(possibilities[:other_product_service_details])
    end

    @class Bookings_services begin
        services ~ Services
    end

    @class Invoice_items begin
        invoices ~ Invoices
        bookings_services ~ Bookings_services
        order_items ~ Order_items
        order_quantity ~ ChooseUniformly(possibilities[:order_quantity])
        other_item_details ~ ChooseUniformly(possibilities[:other_item_details])
    end

    @class Obs begin
        performers_in_bookings ~ Performers_in_bookings
        invoice_items ~ Invoice_items
    end
end

query = @query CreDramaWorkshopGroupsModel.Obs [
    reference_payment_methods_payment_method_code invoice_items.invoices.reference_payment_methods.payment_method_code
    reference_payment_methods_payment_method_description invoice_items.invoices.reference_payment_methods.payment_method_description
    reference_service_types_service_type_code invoice_items.bookings_services.services.reference_service_types.service_type_code
    reference_service_types_parent_service_type_code invoice_items.bookings_services.services.reference_service_types.parent_service_type_code
    reference_service_types_service_type_description invoice_items.bookings_services.services.reference_service_types.service_type_description
    addresses_address_id performers_in_bookings.bookings.drama_workshop_groups.addresses.address_id
    addresses_line_1 performers_in_bookings.bookings.drama_workshop_groups.addresses.line_1
    addresses_line_2 performers_in_bookings.bookings.drama_workshop_groups.addresses.line_2
    addresses_city_town performers_in_bookings.bookings.drama_workshop_groups.addresses.city_town
    addresses_state_county performers_in_bookings.bookings.drama_workshop_groups.addresses.state_county
    addresses_other_details performers_in_bookings.bookings.drama_workshop_groups.addresses.other_details
    products_product_id invoice_items.order_items.products.product_id
    products_product_name invoice_items.order_items.products.product_name
    products_product_price invoice_items.order_items.products.product_price
    products_product_description invoice_items.order_items.products.product_description
    products_other_product_service_details invoice_items.order_items.products.other_product_service_details
    marketing_regions_marketing_region_code invoice_items.invoices.customer_orders.stores.marketing_regions.marketing_region_code
    marketing_regions_marketing_region_name invoice_items.invoices.customer_orders.stores.marketing_regions.marketing_region_name
    marketing_regions_marketing_region_descriptrion invoice_items.invoices.customer_orders.stores.marketing_regions.marketing_region_descriptrion
    marketing_regions_other_details invoice_items.invoices.customer_orders.stores.marketing_regions.other_details
    clients_client_id performers_in_bookings.bookings.clients.client_id
    clients_customer_email_address performers_in_bookings.bookings.clients.customer_email_address
    clients_customer_name performers_in_bookings.bookings.clients.customer_name
    clients_customer_phone performers_in_bookings.bookings.clients.customer_phone
    clients_other_details performers_in_bookings.bookings.clients.other_details
    drama_workshop_groups_workshop_group_id performers_in_bookings.bookings.drama_workshop_groups.workshop_group_id
    drama_workshop_groups_currency_code performers_in_bookings.bookings.drama_workshop_groups.currency_code
    drama_workshop_groups_marketing_region_code performers_in_bookings.bookings.drama_workshop_groups.marketing_region_code
    drama_workshop_groups_store_name performers_in_bookings.bookings.drama_workshop_groups.store_name
    drama_workshop_groups_store_phone performers_in_bookings.bookings.drama_workshop_groups.store_phone
    drama_workshop_groups_store_email_address performers_in_bookings.bookings.drama_workshop_groups.store_email_address
    drama_workshop_groups_other_details performers_in_bookings.bookings.drama_workshop_groups.other_details
    performers_performer_id performers_in_bookings.performers.performer_id
    performers_customer_name performers_in_bookings.performers.customer_name
    performers_customer_phone performers_in_bookings.performers.customer_phone
    performers_customer_email_address performers_in_bookings.performers.customer_email_address
    performers_other_details performers_in_bookings.performers.other_details
    customers_customer_id invoice_items.invoices.customer_orders.customers.customer_id
    customers_customer_name invoice_items.invoices.customer_orders.customers.customer_name
    customers_customer_phone invoice_items.invoices.customer_orders.customers.customer_phone
    customers_customer_email_address invoice_items.invoices.customer_orders.customers.customer_email_address
    customers_other_details invoice_items.invoices.customer_orders.customers.other_details
    stores_store_id invoice_items.invoices.customer_orders.stores.store_id
    stores_store_name invoice_items.invoices.customer_orders.stores.store_name
    stores_store_phone invoice_items.invoices.customer_orders.stores.store_phone
    stores_store_email_address invoice_items.invoices.customer_orders.stores.store_email_address
    stores_other_details invoice_items.invoices.customer_orders.stores.other_details
    bookings_booking_id performers_in_bookings.bookings.booking_id
    bookings_status_code performers_in_bookings.bookings.status_code
    bookings_store_id performers_in_bookings.bookings.store_id
    bookings_order_date performers_in_bookings.bookings.order_date
    bookings_planned_delivery_date performers_in_bookings.bookings.planned_delivery_date
    bookings_actual_delivery_date performers_in_bookings.bookings.actual_delivery_date
    bookings_other_order_details performers_in_bookings.bookings.other_order_details
    customer_orders_order_id invoice_items.invoices.customer_orders.order_id
    customer_orders_order_date invoice_items.invoices.customer_orders.order_date
    customer_orders_planned_delivery_date invoice_items.invoices.customer_orders.planned_delivery_date
    customer_orders_actual_delivery_date invoice_items.invoices.customer_orders.actual_delivery_date
    customer_orders_other_order_details invoice_items.invoices.customer_orders.other_order_details
    order_items_order_item_id invoice_items.order_items.order_item_id
    order_items_order_quantity invoice_items.order_items.order_quantity
    order_items_other_item_details invoice_items.order_items.other_item_details
    invoices_invoice_id invoice_items.invoices.invoice_id
    invoices_product_id invoice_items.invoices.product_id
    invoices_order_quantity invoice_items.invoices.order_quantity
    invoices_other_item_details invoice_items.invoices.other_item_details
    invoices_order_item_id invoice_items.invoices.order_item_id
    services_service_id invoice_items.bookings_services.services.service_id
    services_product_description invoice_items.bookings_services.services.product_description
    services_product_name invoice_items.bookings_services.services.product_name
    services_product_price invoice_items.bookings_services.services.product_price
    services_other_product_service_details invoice_items.bookings_services.services.other_product_service_details
    invoice_items_invoice_item_id invoice_items.invoice_item_id
    invoice_items_order_quantity invoice_items.order_quantity
    invoice_items_other_item_details invoice_items.other_item_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
