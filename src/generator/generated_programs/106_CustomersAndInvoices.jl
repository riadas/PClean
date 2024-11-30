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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "order id"], Any[1, "customer id"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice number"], Any[2, "order id"], Any[2, "invoice date"], Any[3, "account id"], Any[3, "customer id"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "production type code"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "product id"], Any[5, "parent product id"], Any[5, "production type code"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "account id"], Any[6, "invoice number"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "order item id"], Any[7, "order id"], Any[7, "product id"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "order item id"], Any[8, "invoice number"], Any[8, "product id"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "order id"], Any[1, "customer id"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice number"], Any[2, "order id"], Any[2, "invoice date"], Any[3, "account id"], Any[3, "customer id"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "production type code"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "product id"], Any[5, "parent product id"], Any[5, "production type code"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "account id"], Any[6, "invoice number"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "order item id"], Any[7, "order id"], Any[7, "product id"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "order item id"], Any[8, "invoice number"], Any[8, "product id"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["customer id", "order id", "customer id", "production type code", "account id", "invoice number", "order id", "product id", "product id", "invoice number", "order item id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "customer first name"], Any[0, "customer middle initial"], Any[0, "customer last name"], Any[0, "gender"], Any[0, "email address"], Any[0, "login name"], Any[0, "login password"], Any[0, "phone number"], Any[0, "town city"], Any[0, "state county province"], Any[0, "country"], Any[1, "date order placed"], Any[1, "order details"], Any[2, "invoice date"], Any[3, "date account opened"], Any[3, "account name"], Any[3, "other account details"], Any[4, "product type description"], Any[4, "vat rating"], Any[5, "parent product id"], Any[5, "unit price"], Any[5, "product name"], Any[5, "product color"], Any[5, "product size"], Any[6, "transaction id"], Any[6, "transaction type"], Any[6, "transaction date"], Any[6, "transaction amount"], Any[6, "transaction comment"], Any[6, "other transaction details"], Any[7, "product quantity"], Any[7, "other order item details"], Any[8, "product title"], Any[8, "product quantity"], Any[8, "product price"], Any[8, "derived product cost"], Any[8, "derived vat payable"], Any[8, "derived total cost"]]
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





PClean.@model CustomersAndInvoicesModel begin
    @class Customers begin
        customer_id ~ Unmodeled()
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

    @class Product_Categories begin
        production_type_code ~ ChooseUniformly(possibilities[:production_type_code])
        product_type_description ~ ChooseUniformly(possibilities[:product_type_description])
        vat_rating ~ ChooseUniformly(possibilities[:vat_rating])
    end

    @class Obs begin
        customers ~ Customers
        product_Categories ~ Product_Categories
        order_id ~ Unmodeled()
        date_order_placed ~ TimePrior(possibilities[:date_order_placed])
        order_details ~ ChooseUniformly(possibilities[:order_details])
        invoice_number ~ ChooseUniformly(possibilities[:invoice_number])
        invoice_date ~ TimePrior(possibilities[:invoice_date])
        account_id ~ Unmodeled()
        date_account_opened ~ TimePrior(possibilities[:date_account_opened])
        account_name ~ ChooseUniformly(possibilities[:account_name])
        other_account_details ~ ChooseUniformly(possibilities[:other_account_details])
        product_id ~ Unmodeled()
        parent_product_id ~ ChooseUniformly(possibilities[:parent_product_id])
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_color ~ ChooseUniformly(possibilities[:product_color])
        product_size ~ ChooseUniformly(possibilities[:product_size])
        transaction_id ~ Unmodeled()
        transaction_type ~ ChooseUniformly(possibilities[:transaction_type])
        transaction_date ~ TimePrior(possibilities[:transaction_date])
        transaction_amount ~ ChooseUniformly(possibilities[:transaction_amount])
        transaction_comment ~ ChooseUniformly(possibilities[:transaction_comment])
        other_transaction_details ~ ChooseUniformly(possibilities[:other_transaction_details])
        order_item_id ~ Unmodeled()
        product_quantity ~ ChooseUniformly(possibilities[:product_quantity])
        other_order_item_details ~ ChooseUniformly(possibilities[:other_order_item_details])
        product_title ~ ChooseUniformly(possibilities[:product_title])
        product_quantity ~ ChooseUniformly(possibilities[:product_quantity])
        product_price ~ ChooseUniformly(possibilities[:product_price])
        derived_product_cost ~ ChooseUniformly(possibilities[:derived_product_cost])
        derived_vat_payable ~ ChooseUniformly(possibilities[:derived_vat_payable])
        derived_total_cost ~ ChooseUniformly(possibilities[:derived_total_cost])
    end
end

query = @query CustomersAndInvoicesModel.Obs [
    customers_customer_id customers.customer_id
    customers_customer_first_name customers.customer_first_name
    customers_customer_middle_initial customers.customer_middle_initial
    customers_customer_last_name customers.customer_last_name
    customers_gender customers.gender
    customers_email_address customers.email_address
    customers_login_name customers.login_name
    customers_login_password customers.login_password
    customers_phone_number customers.phone_number
    customers_town_city customers.town_city
    customers_state_county_province customers.state_county_province
    customers_country customers.country
    orders_order_id order_id
    orders_date_order_placed date_order_placed
    orders_order_details order_details
    invoices_invoice_number invoice_number
    invoices_invoice_date invoice_date
    accounts_account_id account_id
    accounts_date_account_opened date_account_opened
    accounts_account_name account_name
    accounts_other_account_details other_account_details
    product_categories_production_type_code product_Categories.production_type_code
    product_categories_product_type_description product_Categories.product_type_description
    product_categories_vat_rating product_Categories.vat_rating
    products_product_id product_id
    products_parent_product_id parent_product_id
    products_unit_price unit_price
    products_product_name product_name
    products_product_color product_color
    products_product_size product_size
    financial_transactions_transaction_id transaction_id
    financial_transactions_transaction_type transaction_type
    financial_transactions_transaction_date transaction_date
    financial_transactions_transaction_amount transaction_amount
    financial_transactions_transaction_comment transaction_comment
    financial_transactions_other_transaction_details other_transaction_details
    order_items_order_item_id order_item_id
    order_items_product_quantity product_quantity
    order_items_other_order_item_details other_order_item_details
    invoice_line_items_product_title product_title
    invoice_line_items_product_quantity product_quantity
    invoice_line_items_product_price product_price
    invoice_line_items_derived_product_cost derived_product_cost
    invoice_line_items_derived_vat_payable derived_vat_payable
    invoice_line_items_derived_total_cost derived_total_cost
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
