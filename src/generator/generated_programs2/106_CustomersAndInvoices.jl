using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "order id"], Any[1, "customer id"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice number"], Any[2, "order id"], Any[2, "invoice date"], Any[3, "account id"], Any[3, "customer id"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "production type code"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "product id"], Any[5, "parent product id"], Any[5, "production type code"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "account id"], Any[6, "invoice number"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "order item id"], Any[7, "order id"], Any[7, "product id"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "order item id"], Any[8, "invoice number"], Any[8, "product id"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "order id"], Any[1, "customer id"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice number"], Any[2, "order id"], Any[2, "invoice date"], Any[3, "account id"], Any[3, "customer id"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "production type code"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "product id"], Any[5, "parent product id"], Any[5, "production type code"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "account id"], Any[6, "invoice number"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "order item id"], Any[7, "order id"], Any[7, "product id"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "order item id"], Any[8, "invoice number"], Any[8, "product id"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "order id"], Any[1, "customer id"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice number"], Any[2, "order id"], Any[2, "invoice date"], Any[3, "account id"], Any[3, "customer id"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "production type code"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "product id"], Any[5, "parent product id"], Any[5, "production type code"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "account id"], Any[6, "invoice number"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "order item id"], Any[7, "order id"], Any[7, "product id"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "order item id"], Any[8, "invoice number"], Any[8, "product id"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "order id"], Any[1, "customer id"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice number"], Any[2, "order id"], Any[2, "invoice date"], Any[3, "account id"], Any[3, "customer id"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "production type code"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "product id"], Any[5, "parent product id"], Any[5, "production type code"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "account id"], Any[6, "invoice number"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "order item id"], Any[7, "order id"], Any[7, "product id"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "order item id"], Any[8, "invoice number"], Any[8, "product id"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "order id"], Any[1, "customer id"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice number"], Any[2, "order id"], Any[2, "invoice date"], Any[3, "account id"], Any[3, "customer id"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "production type code"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "product id"], Any[5, "parent product id"], Any[5, "production type code"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "account id"], Any[6, "invoice number"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "order item id"], Any[7, "order id"], Any[7, "product id"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "order item id"], Any[8, "invoice number"], Any[8, "product id"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[14, 1], Any[18, 13], Any[21, 1], Any[30, 25], Any[36, 20], Any[37, 17], Any[44, 13], Any[45, 28], Any[50, 28], Any[49, 17], Any[48, 43]])
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







PClean.@model CustomersAndInvoicesModel begin
    @class Customers begin
        customer_first_name ~ ChooseUniformly(possibilities[:customer_first_name])
        customer_middle_initial ~ ChooseUniformly(possibilities[:customer_middle_initial])
        customer_last_name ~ ChooseUniformly(possibilities[:customer_last_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        login_name ~ ChooseUniformly(possibilities[:login_name])
        login_password ~ ChooseUniformly(possibilities[:login_password])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        town_city ~ ChooseUniformly(possibilities[:town_city])
        state_county_province ~ ChooseUniformly(possibilities[:state_county_province])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Orders begin
        customers ~ Customers
        date_order_placed ~ TimePrior(possibilities[:date_order_placed])
        order_details ~ ChooseUniformly(possibilities[:order_details])
    end

    @class Invoices begin
        invoice_number ~ ChooseUniformly(possibilities[:invoice_number])
        orders ~ Orders
        invoice_date ~ TimePrior(possibilities[:invoice_date])
    end

    @class Accounts begin
        customers ~ Customers
        date_account_opened ~ TimePrior(possibilities[:date_account_opened])
        account_name ~ ChooseUniformly(possibilities[:account_name])
        other_account_details ~ ChooseUniformly(possibilities[:other_account_details])
    end

    @class Product_categories begin
        production_type_code ~ ChooseUniformly(possibilities[:production_type_code])
        product_type_description ~ ChooseUniformly(possibilities[:product_type_description])
        vat_rating ~ ChooseUniformly(possibilities[:vat_rating])
    end

    @class Products begin
        parent_product_id ~ Unmodeled()
        product_categories ~ Product_categories
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_color ~ ChooseUniformly(possibilities[:product_color])
        product_size ~ ChooseUniformly(possibilities[:product_size])
    end

    @class Financial_transactions begin
        transaction_id ~ Unmodeled()
        accounts ~ Accounts
        invoices ~ Invoices
        transaction_type ~ ChooseUniformly(possibilities[:transaction_type])
        transaction_date ~ TimePrior(possibilities[:transaction_date])
        transaction_amount ~ ChooseUniformly(possibilities[:transaction_amount])
        transaction_comment ~ ChooseUniformly(possibilities[:transaction_comment])
        other_transaction_details ~ ChooseUniformly(possibilities[:other_transaction_details])
    end

    @class Order_items begin
        orders ~ Orders
        products ~ Products
        product_quantity ~ ChooseUniformly(possibilities[:product_quantity])
        other_order_item_details ~ ChooseUniformly(possibilities[:other_order_item_details])
    end

    @class Invoice_line_items begin
        order_items ~ Order_items
        invoices ~ Invoices
        products ~ Products
        product_title ~ ChooseUniformly(possibilities[:product_title])
        product_quantity ~ ChooseUniformly(possibilities[:product_quantity])
        product_price ~ ChooseUniformly(possibilities[:product_price])
        derived_product_cost ~ ChooseUniformly(possibilities[:derived_product_cost])
        derived_vat_payable ~ ChooseUniformly(possibilities[:derived_vat_payable])
        derived_total_cost ~ ChooseUniformly(possibilities[:derived_total_cost])
    end

    @class Obs begin
        financial_transactions ~ Financial_transactions
        invoice_line_items ~ Invoice_line_items
    end
end

query = @query CustomersAndInvoicesModel.Obs [
    customers_customer_id financial_transactions.invoices.orders.customers.customer_id
    customers_customer_first_name financial_transactions.invoices.orders.customers.customer_first_name
    customers_customer_middle_initial financial_transactions.invoices.orders.customers.customer_middle_initial
    customers_customer_last_name financial_transactions.invoices.orders.customers.customer_last_name
    customers_gender financial_transactions.invoices.orders.customers.gender
    customers_email_address financial_transactions.invoices.orders.customers.email_address
    customers_login_name financial_transactions.invoices.orders.customers.login_name
    customers_login_password financial_transactions.invoices.orders.customers.login_password
    customers_phone_number financial_transactions.invoices.orders.customers.phone_number
    customers_town_city financial_transactions.invoices.orders.customers.town_city
    customers_state_county_province financial_transactions.invoices.orders.customers.state_county_province
    customers_country financial_transactions.invoices.orders.customers.country
    orders_order_id financial_transactions.invoices.orders.order_id
    orders_date_order_placed financial_transactions.invoices.orders.date_order_placed
    orders_order_details financial_transactions.invoices.orders.order_details
    invoices_invoice_number financial_transactions.invoices.invoice_number
    invoices_invoice_date financial_transactions.invoices.invoice_date
    accounts_account_id financial_transactions.accounts.account_id
    accounts_date_account_opened financial_transactions.accounts.date_account_opened
    accounts_account_name financial_transactions.accounts.account_name
    accounts_other_account_details financial_transactions.accounts.other_account_details
    product_categories_production_type_code invoice_line_items.order_items.products.product_categories.production_type_code
    product_categories_product_type_description invoice_line_items.order_items.products.product_categories.product_type_description
    product_categories_vat_rating invoice_line_items.order_items.products.product_categories.vat_rating
    products_product_id invoice_line_items.order_items.products.product_id
    products_parent_product_id invoice_line_items.order_items.products.parent_product_id
    products_unit_price invoice_line_items.order_items.products.unit_price
    products_product_name invoice_line_items.order_items.products.product_name
    products_product_color invoice_line_items.order_items.products.product_color
    products_product_size invoice_line_items.order_items.products.product_size
    financial_transactions_transaction_id financial_transactions.transaction_id
    financial_transactions_transaction_type financial_transactions.transaction_type
    financial_transactions_transaction_date financial_transactions.transaction_date
    financial_transactions_transaction_amount financial_transactions.transaction_amount
    financial_transactions_transaction_comment financial_transactions.transaction_comment
    financial_transactions_other_transaction_details financial_transactions.other_transaction_details
    order_items_order_item_id invoice_line_items.order_items.order_item_id
    order_items_product_quantity invoice_line_items.order_items.product_quantity
    order_items_other_order_item_details invoice_line_items.order_items.other_order_item_details
    invoice_line_items_product_title invoice_line_items.product_title
    invoice_line_items_product_quantity invoice_line_items.product_quantity
    invoice_line_items_product_price invoice_line_items.product_price
    invoice_line_items_derived_product_cost invoice_line_items.derived_product_cost
    invoice_line_items_derived_vat_payable invoice_line_items.derived_vat_payable
    invoice_line_items_derived_total_cost invoice_line_items.derived_total_cost
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
