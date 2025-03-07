using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("investors_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("investors_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "investor id"], Any[0, "investor details"], Any[1, "lot id"], Any[1, "investor id"], Any[1, "lot details"], Any[2, "transaction type code"], Any[2, "transaction type description"], Any[3, "transaction id"], Any[3, "investor id"], Any[3, "transaction type code"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales transaction id"], Any[4, "sales details"], Any[5, "purchase transaction id"], Any[5, "purchase details"], Any[6, "transaction id"], Any[6, "lot id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "investor id"], Any[0, "investor details"], Any[1, "lot id"], Any[1, "investor id"], Any[1, "lot details"], Any[2, "transaction type code"], Any[2, "transaction type description"], Any[3, "transaction id"], Any[3, "investor id"], Any[3, "transaction type code"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales transaction id"], Any[4, "sales details"], Any[5, "purchase transaction id"], Any[5, "purchase details"], Any[6, "transaction id"], Any[6, "lot id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "investor id"], Any[0, "investor details"], Any[1, "lot id"], Any[1, "investor id"], Any[1, "lot details"], Any[2, "transaction type code"], Any[2, "transaction type description"], Any[3, "transaction id"], Any[3, "investor id"], Any[3, "transaction type code"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales transaction id"], Any[4, "sales details"], Any[5, "purchase transaction id"], Any[5, "purchase details"], Any[6, "transaction id"], Any[6, "lot id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "investor id"], Any[0, "investor details"], Any[1, "lot id"], Any[1, "investor id"], Any[1, "lot details"], Any[2, "transaction type code"], Any[2, "transaction type description"], Any[3, "transaction id"], Any[3, "investor id"], Any[3, "transaction type code"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales transaction id"], Any[4, "sales details"], Any[5, "purchase transaction id"], Any[5, "purchase details"], Any[6, "transaction id"], Any[6, "lot id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "investor id"], Any[0, "investor details"], Any[1, "lot id"], Any[1, "investor id"], Any[1, "lot details"], Any[2, "transaction type code"], Any[2, "transaction type description"], Any[3, "transaction id"], Any[3, "investor id"], Any[3, "transaction type code"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales transaction id"], Any[4, "sales details"], Any[5, "purchase transaction id"], Any[5, "purchase details"], Any[6, "transaction id"], Any[6, "lot id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[4, 1], Any[10, 6], Any[9, 1], Any[15, 8], Any[17, 8], Any[19, 8], Any[20, 3]])
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







PClean.@model TrackingShareTransactionsModel begin
    @class Investors begin
        investor_details ~ ChooseUniformly(possibilities[:investor_details])
    end

    @class Lots begin
        investors ~ Investors
        lot_details ~ ChooseUniformly(possibilities[:lot_details])
    end

    @class Reference_transaction_types begin
        transaction_type_code ~ ChooseUniformly(possibilities[:transaction_type_code])
        transaction_type_description ~ ChooseUniformly(possibilities[:transaction_type_description])
    end

    @class Transactions begin
        investors ~ Investors
        reference_transaction_types ~ Reference_transaction_types
        date_of_transaction ~ TimePrior(possibilities[:date_of_transaction])
        amount_of_transaction ~ ChooseUniformly(possibilities[:amount_of_transaction])
        share_count ~ ChooseUniformly(possibilities[:share_count])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Sales begin
        sales_details ~ ChooseUniformly(possibilities[:sales_details])
    end

    @class Purchases begin
        transactions ~ Transactions
        purchase_details ~ ChooseUniformly(possibilities[:purchase_details])
    end

    @class Transactions_lots begin
        transactions ~ Transactions
        lots ~ Lots
    end

    @class Obs begin
        sales ~ Sales
        purchases ~ Purchases
        transactions_lots ~ Transactions_lots
    end
end

query = @query TrackingShareTransactionsModel.Obs [
    investors_investor_id transactions_lots.transactions.investors.investor_id
    investors_investor_details transactions_lots.transactions.investors.investor_details
    lots_lot_id transactions_lots.lots.lot_id
    lots_lot_details transactions_lots.lots.lot_details
    reference_transaction_types_transaction_type_code sales.transactions.reference_transaction_types.transaction_type_code
    reference_transaction_types_transaction_type_description sales.transactions.reference_transaction_types.transaction_type_description
    transactions_transaction_id sales.transactions.transaction_id
    transactions_date_of_transaction sales.transactions.date_of_transaction
    transactions_amount_of_transaction sales.transactions.amount_of_transaction
    transactions_share_count sales.transactions.share_count
    transactions_other_details sales.transactions.other_details
    sales_details sales.sales_details
    purchases_purchase_details purchases.purchase_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
