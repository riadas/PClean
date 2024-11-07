using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "staff id"], Any[1, "staff gender"], Any[1, "staff name"], Any[2, "supplier id"], Any[2, "supplier name"], Any[2, "supplier phone"], Any[3, "department store chain id"], Any[3, "department store chain name"], Any[4, "customer id"], Any[4, "payment method code"], Any[4, "customer code"], Any[4, "customer name"], Any[4, "customer address"], Any[4, "customer phone"], Any[4, "customer email"], Any[5, "product id"], Any[5, "product type code"], Any[5, "product name"], Any[5, "product price"], Any[6, "supplier id"], Any[6, "address id"], Any[6, "date from"], Any[6, "date to"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "date to"], Any[8, "order id"], Any[8, "customer id"], Any[8, "order status code"], Any[8, "order date"], Any[9, "department store id"], Any[9, "department store chain id"], Any[9, "store name"], Any[9, "store address"], Any[9, "store phone"], Any[9, "store email"], Any[10, "department id"], Any[10, "department store id"], Any[10, "department name"], Any[11, "order item id"], Any[11, "order id"], Any[11, "product id"], Any[12, "product id"], Any[12, "supplier id"], Any[12, "date supplied from"], Any[12, "date supplied to"], Any[12, "total amount purchased"], Any[12, "total value purchased"], Any[13, "staff id"], Any[13, "department id"], Any[13, "date assigned from"], Any[13, "job title code"], Any[13, "date assigned to"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "staff id"], Any[1, "staff gender"], Any[1, "staff name"], Any[2, "supplier id"], Any[2, "supplier name"], Any[2, "supplier phone"], Any[3, "department store chain id"], Any[3, "department store chain name"], Any[4, "customer id"], Any[4, "payment method code"], Any[4, "customer code"], Any[4, "customer name"], Any[4, "customer address"], Any[4, "customer phone"], Any[4, "customer email"], Any[5, "product id"], Any[5, "product type code"], Any[5, "product name"], Any[5, "product price"], Any[6, "supplier id"], Any[6, "address id"], Any[6, "date from"], Any[6, "date to"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "date to"], Any[8, "order id"], Any[8, "customer id"], Any[8, "order status code"], Any[8, "order date"], Any[9, "department store id"], Any[9, "department store chain id"], Any[9, "store name"], Any[9, "store address"], Any[9, "store phone"], Any[9, "store email"], Any[10, "department id"], Any[10, "department store id"], Any[10, "department name"], Any[11, "order item id"], Any[11, "order id"], Any[11, "product id"], Any[12, "product id"], Any[12, "supplier id"], Any[12, "date supplied from"], Any[12, "date supplied to"], Any[12, "total amount purchased"], Any[12, "total value purchased"], Any[13, "staff id"], Any[13, "department id"], Any[13, "date assigned from"], Any[13, "job title code"], Any[13, "date assigned to"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model DepartmentStoreModel begin
    @class Addresses begin
        address_id ~ Unmodeled()
        address_details ~ ChooseUniformly(possibilities[:address_details])
    end

    @class Staff begin
        staff_id ~ Unmodeled()
        staff_gender ~ ChooseUniformly(possibilities[:staff_gender])
        staff_name ~ ChooseUniformly(possibilities[:staff_name])
    end

    @class Suppliers begin
        supplier_id ~ Unmodeled()
        supplier_name ~ ChooseUniformly(possibilities[:supplier_name])
        supplier_phone ~ ChooseUniformly(possibilities[:supplier_phone])
    end

    @class Department_Store_Chain begin
        department_store_chain_id ~ Unmodeled()
        department_store_chain_name ~ ChooseUniformly(possibilities[:department_store_chain_name])
    end

    @class Customers begin
        customer_id ~ Unmodeled()
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        customer_code ~ ChooseUniformly(possibilities[:customer_code])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_address ~ ChooseUniformly(possibilities[:customer_address])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
    end

    @class Products begin
        product_id ~ Unmodeled()
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
    end

    @class Supplier_Addresses begin
        supplier_id ~ Unmodeled()
        address_id ~ ChooseUniformly(possibilities[:address_id])
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
    end

    @class Customer_Addresses begin
        customer_id ~ Unmodeled()
        address_id ~ ChooseUniformly(possibilities[:address_id])
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
    end

    @class Customer_Orders begin
        order_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
        order_date ~ TimePrior(possibilities[:order_date])
    end

    @class Department_Stores begin
        department_store_id ~ Unmodeled()
        department_store_chain_id ~ ChooseUniformly(possibilities[:department_store_chain_id])
        store_name ~ ChooseUniformly(possibilities[:store_name])
        store_address ~ ChooseUniformly(possibilities[:store_address])
        store_phone ~ ChooseUniformly(possibilities[:store_phone])
        store_email ~ ChooseUniformly(possibilities[:store_email])
    end

    @class Departments begin
        department_id ~ Unmodeled()
        department_store_id ~ ChooseUniformly(possibilities[:department_store_id])
        department_name ~ ChooseUniformly(possibilities[:department_name])
    end

    @class Order_Items begin
        order_item_id ~ Unmodeled()
        order_id ~ ChooseUniformly(possibilities[:order_id])
        product_id ~ ChooseUniformly(possibilities[:product_id])
    end

    @class Product_Suppliers begin
        product_id ~ Unmodeled()
        supplier_id ~ ChooseUniformly(possibilities[:supplier_id])
        date_supplied_from ~ TimePrior(possibilities[:date_supplied_from])
        date_supplied_to ~ TimePrior(possibilities[:date_supplied_to])
        total_amount_purchased ~ ChooseUniformly(possibilities[:total_amount_purchased])
        total_value_purchased ~ ChooseUniformly(possibilities[:total_value_purchased])
    end

    @class Staff_Department_Assignments begin
        staff_id ~ Unmodeled()
        department_id ~ ChooseUniformly(possibilities[:department_id])
        date_assigned_from ~ TimePrior(possibilities[:date_assigned_from])
        job_title_code ~ ChooseUniformly(possibilities[:job_title_code])
        date_assigned_to ~ TimePrior(possibilities[:date_assigned_to])
    end

    @class Obs begin
        addresses ~ Addresses
        staff ~ Staff
        suppliers ~ Suppliers
        department_Store_Chain ~ Department_Store_Chain
        customers ~ Customers
        products ~ Products
        supplier_Addresses ~ Supplier_Addresses
        customer_Addresses ~ Customer_Addresses
        customer_Orders ~ Customer_Orders
        department_Stores ~ Department_Stores
        departments ~ Departments
        order_Items ~ Order_Items
        product_Suppliers ~ Product_Suppliers
        staff_Department_Assignments ~ Staff_Department_Assignments
    end
end

query = @query DepartmentStoreModel.Obs [
    addresses_address_id addresses.address_id
    addresses_address_details addresses.address_details
    staff_id staff.staff_id
    staff_gender staff.staff_gender
    staff_name staff.staff_name
    suppliers_supplier_id suppliers.supplier_id
    suppliers_supplier_name suppliers.supplier_name
    suppliers_supplier_phone suppliers.supplier_phone
    department_store_chain_id department_Store_Chain.department_store_chain_id
    department_store_chain_name department_Store_Chain.department_store_chain_name
    customers_customer_id customers.customer_id
    customers_payment_method_code customers.payment_method_code
    customers_customer_code customers.customer_code
    customers_customer_name customers.customer_name
    customers_customer_address customers.customer_address
    customers_customer_phone customers.customer_phone
    customers_customer_email customers.customer_email
    products_product_id products.product_id
    products_product_type_code products.product_type_code
    products_product_name products.product_name
    products_product_price products.product_price
    supplier_addresses_date_from supplier_Addresses.date_from
    supplier_addresses_date_to supplier_Addresses.date_to
    customer_addresses_date_from customer_Addresses.date_from
    customer_addresses_date_to customer_Addresses.date_to
    customer_orders_order_id customer_Orders.order_id
    customer_orders_order_status_code customer_Orders.order_status_code
    customer_orders_order_date customer_Orders.order_date
    department_stores_department_store_id department_Stores.department_store_id
    department_stores_store_name department_Stores.store_name
    department_stores_store_address department_Stores.store_address
    department_stores_store_phone department_Stores.store_phone
    department_stores_store_email department_Stores.store_email
    departments_department_id departments.department_id
    departments_department_name departments.department_name
    order_items_order_item_id order_Items.order_item_id
    product_suppliers_date_supplied_from product_Suppliers.date_supplied_from
    product_suppliers_date_supplied_to product_Suppliers.date_supplied_to
    product_suppliers_total_amount_purchased product_Suppliers.total_amount_purchased
    product_suppliers_total_value_purchased product_Suppliers.total_value_purchased
    staff_department_assignments_date_assigned_from staff_Department_Assignments.date_assigned_from
    staff_department_assignments_job_title_code staff_Department_Assignments.job_title_code
    staff_department_assignments_date_assigned_to staff_Department_Assignments.date_assigned_to
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
