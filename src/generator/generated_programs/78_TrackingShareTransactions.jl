using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("investors_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("investors_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "investor id"], Any[0, "investor details"], Any[1, "lot id"], Any[1, "investor id"], Any[1, "lot details"], Any[2, "transaction type code"], Any[2, "transaction type description"], Any[3, "transaction id"], Any[3, "investor id"], Any[3, "transaction type code"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales transaction id"], Any[4, "sales details"], Any[5, "purchase transaction id"], Any[5, "purchase details"], Any[6, "transaction id"], Any[6, "lot id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "investor id"], Any[0, "investor details"], Any[1, "lot id"], Any[1, "investor id"], Any[1, "lot details"], Any[2, "transaction type code"], Any[2, "transaction type description"], Any[3, "transaction id"], Any[3, "investor id"], Any[3, "transaction type code"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales transaction id"], Any[4, "sales details"], Any[5, "purchase transaction id"], Any[5, "purchase details"], Any[6, "transaction id"], Any[6, "lot id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model TrackingShareTransactionsModel begin
    @class Investors begin
        investor_id ~ Unmodeled()
        investor_details ~ ChooseUniformly(possibilities[:investor_details])
    end

    @class Lots begin
        lot_id ~ Unmodeled()
        investor_id ~ ChooseUniformly(possibilities[:investor_id])
        lot_details ~ ChooseUniformly(possibilities[:lot_details])
    end

    @class Reference_Transaction_Types begin
        transaction_type_code ~ ChooseUniformly(possibilities[:transaction_type_code])
        transaction_type_description ~ ChooseUniformly(possibilities[:transaction_type_description])
    end

    @class Transactions begin
        transaction_id ~ Unmodeled()
        investor_id ~ ChooseUniformly(possibilities[:investor_id])
        transaction_type_code ~ ChooseUniformly(possibilities[:transaction_type_code])
        date_of_transaction ~ TimePrior(possibilities[:date_of_transaction])
        amount_of_transaction ~ ChooseUniformly(possibilities[:amount_of_transaction])
        share_count ~ ChooseUniformly(possibilities[:share_count])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Sales begin
        sales_transaction_id ~ Unmodeled()
        sales_details ~ ChooseUniformly(possibilities[:sales_details])
    end

    @class Purchases begin
        purchase_transaction_id ~ Unmodeled()
        purchase_details ~ ChooseUniformly(possibilities[:purchase_details])
    end

    @class Transactions_Lots begin
        transaction_id ~ Unmodeled()
        lot_id ~ ChooseUniformly(possibilities[:lot_id])
    end

    @class Obs begin
        investors ~ Investors
        lots ~ Lots
        reference_Transaction_Types ~ Reference_Transaction_Types
        transactions ~ Transactions
        sales ~ Sales
        purchases ~ Purchases
        transactions_Lots ~ Transactions_Lots
    end
end

query = @query TrackingShareTransactionsModel.Obs [
    investors_investor_id investors.investor_id
    investors_investor_details investors.investor_details
    lots_lot_id lots.lot_id
    lots_lot_details lots.lot_details
    reference_transaction_types_transaction_type_code reference_Transaction_Types.transaction_type_code
    reference_transaction_types_transaction_type_description reference_Transaction_Types.transaction_type_description
    transactions_transaction_id transactions.transaction_id
    transactions_date_of_transaction transactions.date_of_transaction
    transactions_amount_of_transaction transactions.amount_of_transaction
    transactions_share_count transactions.share_count
    transactions_other_details transactions.other_details
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
