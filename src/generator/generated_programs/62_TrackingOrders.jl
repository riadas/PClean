using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[0, "customer details"], Any[1, "invoice number"], Any[1, "invoice date"], Any[1, "invoice details"], Any[2, "order id"], Any[2, "customer id"], Any[2, "order status"], Any[2, "date order placed"], Any[2, "order details"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "order item id"], Any[4, "product id"], Any[4, "order id"], Any[4, "order item status"], Any[4, "order item details"], Any[5, "shipment id"], Any[5, "order id"], Any[5, "invoice number"], Any[5, "shipment tracking number"], Any[5, "shipment date"], Any[5, "other shipment details"], Any[6, "shipment id"], Any[6, "order item id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[0, "customer details"], Any[1, "invoice number"], Any[1, "invoice date"], Any[1, "invoice details"], Any[2, "order id"], Any[2, "customer id"], Any[2, "order status"], Any[2, "date order placed"], Any[2, "order details"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "order item id"], Any[4, "product id"], Any[4, "order id"], Any[4, "order item status"], Any[4, "order item details"], Any[5, "shipment id"], Any[5, "order id"], Any[5, "invoice number"], Any[5, "shipment tracking number"], Any[5, "shipment date"], Any[5, "other shipment details"], Any[6, "shipment id"], Any[6, "order item id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Orders begin
        order_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        order_status ~ ChooseUniformly(possibilities[:order_status])
        date_order_placed ~ TimePrior(possibilities[:date_order_placed])
        order_details ~ ChooseUniformly(possibilities[:order_details])
    end

    @class Products begin
        product_id ~ Unmodeled()
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_details ~ ChooseUniformly(possibilities[:product_details])
    end

    @class Order_Items begin
        order_item_id ~ Unmodeled()
        product_id ~ ChooseUniformly(possibilities[:product_id])
        order_id ~ ChooseUniformly(possibilities[:order_id])
        order_item_status ~ ChooseUniformly(possibilities[:order_item_status])
        order_item_details ~ ChooseUniformly(possibilities[:order_item_details])
    end

    @class Shipments begin
        shipment_id ~ Unmodeled()
        order_id ~ ChooseUniformly(possibilities[:order_id])
        invoice_number ~ ChooseUniformly(possibilities[:invoice_number])
        shipment_tracking_number ~ ChooseUniformly(possibilities[:shipment_tracking_number])
        shipment_date ~ TimePrior(possibilities[:shipment_date])
        other_shipment_details ~ ChooseUniformly(possibilities[:other_shipment_details])
    end

    @class Shipment_Items begin
        shipment_id ~ Unmodeled()
        order_item_id ~ ChooseUniformly(possibilities[:order_item_id])
    end

    @class Obs begin
        customers ~ Customers
        invoices ~ Invoices
        orders ~ Orders
        products ~ Products
        order_Items ~ Order_Items
        shipments ~ Shipments
        shipment_Items ~ Shipment_Items
    end
end

query = @query TrackingOrdersModel.Obs [
    customers_customer_id customers.customer_id
    customers_customer_name customers.customer_name
    customers_customer_details customers.customer_details
    invoices_invoice_number invoices.invoice_number
    invoices_invoice_date invoices.invoice_date
    invoices_invoice_details invoices.invoice_details
    orders_order_id orders.order_id
    orders_order_status orders.order_status
    orders_date_order_placed orders.date_order_placed
    orders_order_details orders.order_details
    products_product_id products.product_id
    products_product_name products.product_name
    products_product_details products.product_details
    order_items_order_item_id order_Items.order_item_id
    order_items_order_item_status order_Items.order_item_status
    order_items_order_item_details order_Items.order_item_details
    shipments_shipment_id shipments.shipment_id
    shipments_shipment_tracking_number shipments.shipment_tracking_number
    shipments_shipment_date shipments.shipment_date
    shipments_other_shipment_details shipments.other_shipment_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
