using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("bank_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("bank_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "branch id"], Any[0, "bname"], Any[0, "no of customers"], Any[0, "city"], Any[0, "state"], Any[1, "customer id"], Any[1, "customer name"], Any[1, "account type"], Any[1, "account balance"], Any[1, "number of loans"], Any[1, "credit score"], Any[1, "branch id"], Any[1, "state"], Any[2, "loan id"], Any[2, "loan type"], Any[2, "customer id"], Any[2, "branch id"], Any[2, "amount"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 1], Any[17, 1]])
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







PClean.@model Loan1Model begin
    @class Bank begin
        bname ~ ChooseUniformly(possibilities[:bname])
        no_of_customers ~ ChooseUniformly(possibilities[:no_of_customers])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Customer begin
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        account_type ~ ChooseUniformly(possibilities[:account_type])
        account_balance ~ ChooseUniformly(possibilities[:account_balance])
        number_of_loans ~ ChooseUniformly(possibilities[:number_of_loans])
        credit_score ~ ChooseUniformly(possibilities[:credit_score])
        bank ~ Bank
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Loan begin
        loan_id ~ ChooseUniformly(possibilities[:loan_id])
        loan_type ~ ChooseUniformly(possibilities[:loan_type])
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        bank ~ Bank
        amount ~ ChooseUniformly(possibilities[:amount])
    end

    @class Obs begin
        customer ~ Customer
        loan ~ Loan
    end
end

query = @query Loan1Model.Obs [
    bank_branch_id customer.bank.branch_id
    bank_bname customer.bank.bname
    bank_no_of_customers customer.bank.no_of_customers
    bank_city customer.bank.city
    bank_state customer.bank.state
    customer_id customer.customer_id
    customer_name customer.customer_name
    customer_account_type customer.account_type
    customer_account_balance customer.account_balance
    customer_number_of_loans customer.number_of_loans
    customer_credit_score customer.credit_score
    customer_state customer.state
    loan_id loan.loan_id
    loan_type loan.loan_type
    loan_customer_id loan.customer_id
    loan_amount loan.amount
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
