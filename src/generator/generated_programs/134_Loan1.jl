using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("bank_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("bank_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "branch id"], Any[0, "bname"], Any[0, "no of customers"], Any[0, "city"], Any[0, "state"], Any[1, "customer id"], Any[1, "customer name"], Any[1, "account type"], Any[1, "account balance"], Any[1, "number of loans"], Any[1, "credit score"], Any[1, "branch id"], Any[1, "state"], Any[2, "loan id"], Any[2, "loan type"], Any[2, "customer id"], Any[2, "branch id"], Any[2, "amount"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "branch id"], Any[0, "bname"], Any[0, "no of customers"], Any[0, "city"], Any[0, "state"], Any[1, "customer id"], Any[1, "customer name"], Any[1, "account type"], Any[1, "account balance"], Any[1, "number of loans"], Any[1, "credit score"], Any[1, "branch id"], Any[1, "state"], Any[2, "loan id"], Any[2, "loan type"], Any[2, "customer id"], Any[2, "branch id"], Any[2, "amount"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["branch id", "branch id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "bname"], Any[0, "no of customers"], Any[0, "city"], Any[0, "state"], Any[1, "customer id"], Any[1, "customer name"], Any[1, "account type"], Any[1, "account balance"], Any[1, "number of loans"], Any[1, "credit score"], Any[1, "state"], Any[2, "loan id"], Any[2, "loan type"], Any[2, "customer id"], Any[2, "amount"]]
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





PClean.@model Loan1Model begin
    @class Bank begin
        branch_id ~ Unmodeled()
        bname ~ ChooseUniformly(possibilities[:bname])
        no_of_customers ~ ChooseUniformly(possibilities[:no_of_customers])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Obs begin
        bank ~ Bank
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        account_type ~ ChooseUniformly(possibilities[:account_type])
        account_balance ~ ChooseUniformly(possibilities[:account_balance])
        number_of_loans ~ ChooseUniformly(possibilities[:number_of_loans])
        credit_score ~ ChooseUniformly(possibilities[:credit_score])
        state ~ ChooseUniformly(possibilities[:state])
        loan_id ~ ChooseUniformly(possibilities[:loan_id])
        loan_type ~ ChooseUniformly(possibilities[:loan_type])
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        amount ~ ChooseUniformly(possibilities[:amount])
    end
end

query = @query Loan1Model.Obs [
    bank_branch_id bank.branch_id
    bank_bname bank.bname
    bank_no_of_customers bank.no_of_customers
    bank_city bank.city
    bank_state bank.state
    customer_id customer_id
    customer_name customer_name
    customer_account_type account_type
    customer_account_balance account_balance
    customer_number_of_loans number_of_loans
    customer_credit_score credit_score
    customer_state state
    loan_id loan_id
    loan_type loan_type
    loan_customer_id customer_id
    loan_amount amount
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
