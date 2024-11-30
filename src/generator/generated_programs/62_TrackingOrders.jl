using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[0, "customer details"], Any[1, "invoice number"], Any[1, "invoice date"], Any[1, "invoice details"], Any[2, "order id"], Any[2, "customer id"], Any[2, "order status"], Any[2, "date order placed"], Any[2, "order details"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "order item id"], Any[4, "product id"], Any[4, "order id"], Any[4, "order item status"], Any[4, "order item details"], Any[5, "shipment id"], Any[5, "order id"], Any[5, "invoice number"], Any[5, "shipment tracking number"], Any[5, "shipment date"], Any[5, "other shipment details"], Any[6, "shipment id"], Any[6, "order item id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[0, "customer details"], Any[1, "invoice number"], Any[1, "invoice date"], Any[1, "invoice details"], Any[2, "order id"], Any[2, "customer id"], Any[2, "order status"], Any[2, "date order placed"], Any[2, "order details"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "order item id"], Any[4, "product id"], Any[4, "order id"], Any[4, "order item status"], Any[4, "order item details"], Any[5, "shipment id"], Any[5, "order id"], Any[5, "invoice number"], Any[5, "shipment tracking number"], Any[5, "shipment date"], Any[5, "other shipment details"], Any[6, "shipment id"], Any[6, "order item id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["customer id", "product id", "order id", "invoice number", "order id", "shipment id", "order item id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "customer name"], Any[0, "customer details"], Any[1, "invoice date"], Any[1, "invoice details"], Any[2, "order status"], Any[2, "date order placed"], Any[2, "order details"], Any[3, "product name"], Any[3, "product details"], Any[4, "order item status"], Any[4, "order item details"], Any[5, "shipment tracking number"], Any[5, "shipment date"], Any[5, "other shipment details"]]
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





PClean.@model TrackingOrdersModel begin
    @class Customers begin
        customer_id ~ Unmodeled()
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_details ~ ChooseUniformly(possibilities[:customer_details])
    end

    @class Invoices begin
        invoice_number ~ ChooseUniformly(possibilities[:invoice_number])
        invoice_date ~ TimePrior(possibilities[:invoice_date])
        invoice_details ~ ChooseUniformly(possibilities[:invoice_details])
    end

    @class Products begin
        product_id ~ Unmodeled()
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_details ~ ChooseUniformly(possibilities[:product_details])
    end

    @class Obs begin
        customers ~ Customers
        invoices ~ Invoices
        products ~ Products
        order_id ~ Unmodeled()
        order_status ~ ChooseUniformly(possibilities[:order_status])
        date_order_placed ~ TimePrior(possibilities[:date_order_placed])
        order_details ~ ChooseUniformly(possibilities[:order_details])
        order_item_id ~ Unmodeled()
        order_item_status ~ ChooseUniformly(possibilities[:order_item_status])
        order_item_details ~ ChooseUniformly(possibilities[:order_item_details])
        shipment_id ~ Unmodeled()
        shipment_tracking_number ~ ChooseUniformly(possibilities[:shipment_tracking_number])
        shipment_date ~ TimePrior(possibilities[:shipment_date])
        other_shipment_details ~ ChooseUniformly(possibilities[:other_shipment_details])
    end
end

query = @query TrackingOrdersModel.Obs [
    customers_customer_id customers.customer_id
    customers_customer_name customers.customer_name
    customers_customer_details customers.customer_details
    invoices_invoice_number invoices.invoice_number
    invoices_invoice_date invoices.invoice_date
    invoices_invoice_details invoices.invoice_details
    orders_order_id order_id
    orders_order_status order_status
    orders_date_order_placed date_order_placed
    orders_order_details order_details
    products_product_id products.product_id
    products_product_name products.product_name
    products_product_details products.product_details
    order_items_order_item_id order_item_id
    order_items_order_item_status order_item_status
    order_items_order_item_details order_item_details
    shipments_shipment_id shipment_id
    shipments_shipment_tracking_number shipment_tracking_number
    shipments_shipment_date shipment_date
    shipments_other_shipment_details other_shipment_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
