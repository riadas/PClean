using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("investors_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("investors_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["investor id", "transaction type code", "investor id", "sales transaction id", "purchase transaction id", "transaction id", "lot id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "investor details"], Any[1, "lot details"], Any[2, "transaction type description"], Any[3, "date of transaction"], Any[3, "amount of transaction"], Any[3, "share count"], Any[3, "other details"], Any[4, "sales details"], Any[5, "purchase details"]]
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





PClean.@model TrackingShareTransactionsModel begin
    @class Investors begin
        investor_id ~ Unmodeled()
        investor_details ~ ChooseUniformly(possibilities[:investor_details])
    end

    @class Reference_Transaction_Types begin
        transaction_type_code ~ ChooseUniformly(possibilities[:transaction_type_code])
        transaction_type_description ~ ChooseUniformly(possibilities[:transaction_type_description])
    end

    @class Obs begin
        investors ~ Investors
        reference_Transaction_Types ~ Reference_Transaction_Types
        lot_id ~ Unmodeled()
        lot_details ~ ChooseUniformly(possibilities[:lot_details])
        transaction_id ~ Unmodeled()
        date_of_transaction ~ TimePrior(possibilities[:date_of_transaction])
        amount_of_transaction ~ ChooseUniformly(possibilities[:amount_of_transaction])
        share_count ~ ChooseUniformly(possibilities[:share_count])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        sales_details ~ ChooseUniformly(possibilities[:sales_details])
        purchase_details ~ ChooseUniformly(possibilities[:purchase_details])
    end
end

query = @query TrackingShareTransactionsModel.Obs [
    investors_investor_id investors.investor_id
    investors_investor_details investors.investor_details
    lots_lot_id lot_id
    lots_lot_details lot_details
    reference_transaction_types_transaction_type_code reference_Transaction_Types.transaction_type_code
    reference_transaction_types_transaction_type_description reference_Transaction_Types.transaction_type_description
    transactions_transaction_id transaction_id
    transactions_date_of_transaction date_of_transaction
    transactions_amount_of_transaction amount_of_transaction
    transactions_share_count share_count
    transactions_other_details other_details
    sales_details sales_details
    purchases_purchase_details purchase_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
