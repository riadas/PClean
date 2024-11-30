using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("accounts_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("accounts_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "account id"], Any[0, "customer id"], Any[0, "account name"], Any[0, "other account details"], Any[1, "customer id"], Any[1, "customer first name"], Any[1, "customer last name"], Any[1, "customer address"], Any[1, "customer phone"], Any[1, "customer email"], Any[1, "other customer details"], Any[2, "card id"], Any[2, "customer id"], Any[2, "card type code"], Any[2, "card number"], Any[2, "date valid from"], Any[2, "date valid to"], Any[2, "other card details"], Any[3, "transaction id"], Any[3, "previous transaction id"], Any[3, "account id"], Any[3, "card id"], Any[3, "transaction type"], Any[3, "transaction date"], Any[3, "transaction amount"], Any[3, "transaction comment"], Any[3, "other transaction details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "account id"], Any[0, "customer id"], Any[0, "account name"], Any[0, "other account details"], Any[1, "customer id"], Any[1, "customer first name"], Any[1, "customer last name"], Any[1, "customer address"], Any[1, "customer phone"], Any[1, "customer email"], Any[1, "other customer details"], Any[2, "card id"], Any[2, "customer id"], Any[2, "card type code"], Any[2, "card number"], Any[2, "date valid from"], Any[2, "date valid to"], Any[2, "other card details"], Any[3, "transaction id"], Any[3, "previous transaction id"], Any[3, "account id"], Any[3, "card id"], Any[3, "transaction type"], Any[3, "transaction date"], Any[3, "transaction amount"], Any[3, "transaction comment"], Any[3, "other transaction details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["account id", "card id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "account name"], Any[0, "other account details"], Any[1, "customer id"], Any[1, "customer first name"], Any[1, "customer last name"], Any[1, "customer address"], Any[1, "customer phone"], Any[1, "customer email"], Any[1, "other customer details"], Any[2, "customer id"], Any[2, "card type code"], Any[2, "card number"], Any[2, "date valid from"], Any[2, "date valid to"], Any[2, "other card details"], Any[3, "transaction id"], Any[3, "previous transaction id"], Any[3, "transaction type"], Any[3, "transaction date"], Any[3, "transaction amount"], Any[3, "transaction comment"], Any[3, "other transaction details"]]
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





PClean.@model CustomersCardTransactionsModel begin
    @class Accounts begin
        account_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        account_name ~ ChooseUniformly(possibilities[:account_name])
        other_account_details ~ ChooseUniformly(possibilities[:other_account_details])
    end

    @class Customers begin
        customer_id ~ Unmodeled()
        customer_first_name ~ ChooseUniformly(possibilities[:customer_first_name])
        customer_last_name ~ ChooseUniformly(possibilities[:customer_last_name])
        customer_address ~ ChooseUniformly(possibilities[:customer_address])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
        customer_email ~ ChooseUniformly(possibilities[:customer_email])
        other_customer_details ~ ChooseUniformly(possibilities[:other_customer_details])
    end

    @class Customers_Cards begin
        card_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        card_type_code ~ ChooseUniformly(possibilities[:card_type_code])
        card_number ~ ChooseUniformly(possibilities[:card_number])
        date_valid_from ~ TimePrior(possibilities[:date_valid_from])
        date_valid_to ~ TimePrior(possibilities[:date_valid_to])
        other_card_details ~ ChooseUniformly(possibilities[:other_card_details])
    end

    @class Obs begin
        accounts ~ Accounts
        customers ~ Customers
        customers_Cards ~ Customers_Cards
        transaction_id ~ Unmodeled()
        previous_transaction_id ~ ChooseUniformly(possibilities[:previous_transaction_id])
        transaction_type ~ ChooseUniformly(possibilities[:transaction_type])
        transaction_date ~ TimePrior(possibilities[:transaction_date])
        transaction_amount ~ ChooseUniformly(possibilities[:transaction_amount])
        transaction_comment ~ ChooseUniformly(possibilities[:transaction_comment])
        other_transaction_details ~ ChooseUniformly(possibilities[:other_transaction_details])
    end
end

query = @query CustomersCardTransactionsModel.Obs [
    accounts_account_id accounts.account_id
    accounts_customer_id accounts.customer_id
    accounts_account_name accounts.account_name
    accounts_other_account_details accounts.other_account_details
    customers_customer_id customers.customer_id
    customers_customer_first_name customers.customer_first_name
    customers_customer_last_name customers.customer_last_name
    customers_customer_address customers.customer_address
    customers_customer_phone customers.customer_phone
    customers_customer_email customers.customer_email
    customers_other_customer_details customers.other_customer_details
    customers_cards_card_id customers_Cards.card_id
    customers_cards_customer_id customers_Cards.customer_id
    customers_cards_card_type_code customers_Cards.card_type_code
    customers_cards_card_number customers_Cards.card_number
    customers_cards_date_valid_from customers_Cards.date_valid_from
    customers_cards_date_valid_to customers_Cards.date_valid_to
    customers_cards_other_card_details customers_Cards.other_card_details
    financial_transactions_transaction_id transaction_id
    financial_transactions_previous_transaction_id previous_transaction_id
    financial_transactions_transaction_type transaction_type
    financial_transactions_transaction_date transaction_date
    financial_transactions_transaction_amount transaction_amount
    financial_transactions_transaction_comment transaction_comment
    financial_transactions_other_transaction_details other_transaction_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
