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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "staff id"], Any[1, "staff gender"], Any[1, "staff name"], Any[2, "supplier id"], Any[2, "supplier name"], Any[2, "supplier phone"], Any[3, "department store chain id"], Any[3, "department store chain name"], Any[4, "customer id"], Any[4, "payment method code"], Any[4, "customer code"], Any[4, "customer name"], Any[4, "customer address"], Any[4, "customer phone"], Any[4, "customer email"], Any[5, "product id"], Any[5, "product type code"], Any[5, "product name"], Any[5, "product price"], Any[6, "supplier id"], Any[6, "address id"], Any[6, "date from"], Any[6, "date to"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "date to"], Any[8, "order id"], Any[8, "customer id"], Any[8, "order status code"], Any[8, "order date"], Any[9, "department store id"], Any[9, "department store chain id"], Any[9, "store name"], Any[9, "store address"], Any[9, "store phone"], Any[9, "store email"], Any[10, "department id"], Any[10, "department store id"], Any[10, "department name"], Any[11, "order item id"], Any[11, "order id"], Any[11, "product id"], Any[12, "product id"], Any[12, "supplier id"], Any[12, "date supplied from"], Any[12, "date supplied to"], Any[12, "total amount purchased"], Any[12, "total value purchased"], Any[13, "staff id"], Any[13, "department id"], Any[13, "date assigned from"], Any[13, "job title code"], Any[13, "date assigned to"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "staff id"], Any[1, "staff gender"], Any[1, "staff name"], Any[2, "supplier id"], Any[2, "supplier name"], Any[2, "supplier phone"], Any[3, "department store chain id"], Any[3, "department store chain name"], Any[4, "customer id"], Any[4, "payment method code"], Any[4, "customer code"], Any[4, "customer name"], Any[4, "customer address"], Any[4, "customer phone"], Any[4, "customer email"], Any[5, "product id"], Any[5, "product type code"], Any[5, "product name"], Any[5, "product price"], Any[6, "supplier id"], Any[6, "address id"], Any[6, "date from"], Any[6, "date to"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "date to"], Any[8, "order id"], Any[8, "customer id"], Any[8, "order status code"], Any[8, "order date"], Any[9, "department store id"], Any[9, "department store chain id"], Any[9, "store name"], Any[9, "store address"], Any[9, "store phone"], Any[9, "store email"], Any[10, "department id"], Any[10, "department store id"], Any[10, "department name"], Any[11, "order item id"], Any[11, "order id"], Any[11, "product id"], Any[12, "product id"], Any[12, "supplier id"], Any[12, "date supplied from"], Any[12, "date supplied to"], Any[12, "total amount purchased"], Any[12, "total value purchased"], Any[13, "staff id"], Any[13, "department id"], Any[13, "date assigned from"], Any[13, "job title code"], Any[13, "date assigned to"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "staff id"], Any[1, "staff gender"], Any[1, "staff name"], Any[2, "supplier id"], Any[2, "supplier name"], Any[2, "supplier phone"], Any[3, "department store chain id"], Any[3, "department store chain name"], Any[4, "customer id"], Any[4, "payment method code"], Any[4, "customer code"], Any[4, "customer name"], Any[4, "customer address"], Any[4, "customer phone"], Any[4, "customer email"], Any[5, "product id"], Any[5, "product type code"], Any[5, "product name"], Any[5, "product price"], Any[6, "supplier id"], Any[6, "address id"], Any[6, "date from"], Any[6, "date to"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "date to"], Any[8, "order id"], Any[8, "customer id"], Any[8, "order status code"], Any[8, "order date"], Any[9, "department store id"], Any[9, "department store chain id"], Any[9, "store name"], Any[9, "store address"], Any[9, "store phone"], Any[9, "store email"], Any[10, "department id"], Any[10, "department store id"], Any[10, "department name"], Any[11, "order item id"], Any[11, "order id"], Any[11, "product id"], Any[12, "product id"], Any[12, "supplier id"], Any[12, "date supplied from"], Any[12, "date supplied to"], Any[12, "total amount purchased"], Any[12, "total value purchased"], Any[13, "staff id"], Any[13, "department id"], Any[13, "date assigned from"], Any[13, "job title code"], Any[13, "date assigned to"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "staff id"], Any[1, "staff gender"], Any[1, "staff name"], Any[2, "supplier id"], Any[2, "supplier name"], Any[2, "supplier phone"], Any[3, "department store chain id"], Any[3, "department store chain name"], Any[4, "customer id"], Any[4, "payment method code"], Any[4, "customer code"], Any[4, "customer name"], Any[4, "customer address"], Any[4, "customer phone"], Any[4, "customer email"], Any[5, "product id"], Any[5, "product type code"], Any[5, "product name"], Any[5, "product price"], Any[6, "supplier id"], Any[6, "address id"], Any[6, "date from"], Any[6, "date to"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "date to"], Any[8, "order id"], Any[8, "customer id"], Any[8, "order status code"], Any[8, "order date"], Any[9, "department store id"], Any[9, "department store chain id"], Any[9, "store name"], Any[9, "store address"], Any[9, "store phone"], Any[9, "store email"], Any[10, "department id"], Any[10, "department store id"], Any[10, "department name"], Any[11, "order item id"], Any[11, "order id"], Any[11, "product id"], Any[12, "product id"], Any[12, "supplier id"], Any[12, "date supplied from"], Any[12, "date supplied to"], Any[12, "total amount purchased"], Any[12, "total value purchased"], Any[13, "staff id"], Any[13, "department id"], Any[13, "date assigned from"], Any[13, "job title code"], Any[13, "date assigned to"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "staff id"], Any[1, "staff gender"], Any[1, "staff name"], Any[2, "supplier id"], Any[2, "supplier name"], Any[2, "supplier phone"], Any[3, "department store chain id"], Any[3, "department store chain name"], Any[4, "customer id"], Any[4, "payment method code"], Any[4, "customer code"], Any[4, "customer name"], Any[4, "customer address"], Any[4, "customer phone"], Any[4, "customer email"], Any[5, "product id"], Any[5, "product type code"], Any[5, "product name"], Any[5, "product price"], Any[6, "supplier id"], Any[6, "address id"], Any[6, "date from"], Any[6, "date to"], Any[7, "customer id"], Any[7, "address id"], Any[7, "date from"], Any[7, "date to"], Any[8, "order id"], Any[8, "customer id"], Any[8, "order status code"], Any[8, "order date"], Any[9, "department store id"], Any[9, "department store chain id"], Any[9, "store name"], Any[9, "store address"], Any[9, "store phone"], Any[9, "store email"], Any[10, "department id"], Any[10, "department store id"], Any[10, "department name"], Any[11, "order item id"], Any[11, "order id"], Any[11, "product id"], Any[12, "product id"], Any[12, "supplier id"], Any[12, "date supplied from"], Any[12, "date supplied to"], Any[12, "total amount purchased"], Any[12, "total value purchased"], Any[13, "staff id"], Any[13, "department id"], Any[13, "date assigned from"], Any[13, "job title code"], Any[13, "date assigned to"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[22, 6], Any[23, 1], Any[26, 11], Any[27, 1], Any[31, 11], Any[35, 9], Any[41, 34], Any[45, 18], Any[44, 30], Any[46, 18], Any[47, 6], Any[52, 3], Any[53, 40]])
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







PClean.@model DepartmentStoreModel begin
    @class Addresses begin
        address_details ~ ChooseUniformly(possibilities[:address_details])
    end

    @class Staff begin
        staff_gender ~ ChooseUniformly(possibilities[:staff_gender])
        staff_name ~ ChooseUniformly(possibilities[:staff_name])
    end

    @class Suppliers begin
        supplier_name ~ ChooseUniformly(possibilities[:supplier_name])
        supplier_phone ~ ChooseUniformly(possibilities[:supplier_phone])
    end

    @class Department_store_chain begin
        department_store_chain_name ~ ChooseUniformly(possibilities[:department_store_chain_name])
    end

    @class Customers begin
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        customer_code ~ ChooseUniformly(possibilities[:customer_code])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        customer_address ~ ChooseUniformly(possibilities[:customer_address])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
    end

    @class Products begin
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
    end

    @class Supplier_addresses begin
        addresses ~ Addresses
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
    end

    @class Customer_addresses begin
        addresses ~ Addresses
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
    end

    @class Customer_orders begin
        customers ~ Customers
        order_status_code ~ ChooseUniformly(possibilities[:order_status_code])
        order_date ~ TimePrior(possibilities[:order_date])
    end

    @class Department_stores begin
        department_store_chain ~ Department_store_chain
        store_name ~ ChooseUniformly(possibilities[:store_name])
        store_address ~ ChooseUniformly(possibilities[:store_address])
        store_phone ~ ChooseUniformly(possibilities[:store_phone])
        store_email ~ ChooseUniformly(possibilities[:store_email])
    end

    @class Departments begin
        department_stores ~ Department_stores
        department_name ~ ChooseUniformly(possibilities[:department_name])
    end

    @class Order_items begin
        customer_orders ~ Customer_orders
        products ~ Products
    end

    @class Product_suppliers begin
        suppliers ~ Suppliers
        date_supplied_from ~ TimePrior(possibilities[:date_supplied_from])
        date_supplied_to ~ TimePrior(possibilities[:date_supplied_to])
        total_amount_purchased ~ ChooseUniformly(possibilities[:total_amount_purchased])
        total_value_purchased ~ ChooseUniformly(possibilities[:total_value_purchased])
    end

    @class Staff_department_assignments begin
        departments ~ Departments
        date_assigned_from ~ TimePrior(possibilities[:date_assigned_from])
        job_title_code ~ ChooseUniformly(possibilities[:job_title_code])
        date_assigned_to ~ TimePrior(possibilities[:date_assigned_to])
    end

    @class Obs begin
        supplier_addresses ~ Supplier_addresses
        customer_addresses ~ Customer_addresses
        order_items ~ Order_items
        product_suppliers ~ Product_suppliers
        staff_department_assignments ~ Staff_department_assignments
    end
end

query = @query DepartmentStoreModel.Obs [
    addresses_address_id supplier_addresses.addresses.address_id
    addresses_address_details supplier_addresses.addresses.address_details
    staff_id staff_department_assignments.staff.staff_id
    staff_gender staff_department_assignments.staff.staff_gender
    staff_name staff_department_assignments.staff.staff_name
    suppliers_supplier_id supplier_addresses.suppliers.supplier_id
    suppliers_supplier_name supplier_addresses.suppliers.supplier_name
    suppliers_supplier_phone supplier_addresses.suppliers.supplier_phone
    department_store_chain_id staff_department_assignments.departments.department_stores.department_store_chain.department_store_chain_id
    department_store_chain_name staff_department_assignments.departments.department_stores.department_store_chain.department_store_chain_name
    customers_customer_id customer_addresses.customers.customer_id
    customers_payment_method_code customer_addresses.customers.payment_method_code
    customers_customer_code customer_addresses.customers.customer_code
    customers_customer_name customer_addresses.customers.customer_name
    customers_customer_address customer_addresses.customers.customer_address
    customers_customer_phone customer_addresses.customers.customer_phone
    customers_customer_email customer_addresses.customers.customer_email
    products_product_id order_items.products.product_id
    products_product_type_code order_items.products.product_type_code
    products_product_name order_items.products.product_name
    products_product_price order_items.products.product_price
    supplier_addresses_date_from supplier_addresses.date_from
    supplier_addresses_date_to supplier_addresses.date_to
    customer_addresses_date_from customer_addresses.date_from
    customer_addresses_date_to customer_addresses.date_to
    customer_orders_order_id order_items.customer_orders.order_id
    customer_orders_order_status_code order_items.customer_orders.order_status_code
    customer_orders_order_date order_items.customer_orders.order_date
    department_stores_department_store_id staff_department_assignments.departments.department_stores.department_store_id
    department_stores_store_name staff_department_assignments.departments.department_stores.store_name
    department_stores_store_address staff_department_assignments.departments.department_stores.store_address
    department_stores_store_phone staff_department_assignments.departments.department_stores.store_phone
    department_stores_store_email staff_department_assignments.departments.department_stores.store_email
    departments_department_id staff_department_assignments.departments.department_id
    departments_department_name staff_department_assignments.departments.department_name
    order_items_order_item_id order_items.order_item_id
    product_suppliers_date_supplied_from product_suppliers.date_supplied_from
    product_suppliers_date_supplied_to product_suppliers.date_supplied_to
    product_suppliers_total_amount_purchased product_suppliers.total_amount_purchased
    product_suppliers_total_value_purchased product_suppliers.total_value_purchased
    staff_department_assignments_date_assigned_from staff_department_assignments.date_assigned_from
    staff_department_assignments_job_title_code staff_department_assignments.job_title_code
    staff_department_assignments_date_assigned_to staff_department_assignments.date_assigned_to
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
