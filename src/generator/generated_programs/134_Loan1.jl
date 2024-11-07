using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("bank_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("bank_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "branch id"], Any[0, "bname"], Any[0, "no of customers"], Any[0, "city"], Any[0, "state"], Any[1, "customer id"], Any[1, "customer name"], Any[1, "account type"], Any[1, "account balance"], Any[1, "number of loans"], Any[1, "credit score"], Any[1, "branch id"], Any[1, "state"], Any[2, "loan id"], Any[2, "loan type"], Any[2, "customer id"], Any[2, "branch id"], Any[2, "amount"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "branch id"], Any[0, "bname"], Any[0, "no of customers"], Any[0, "city"], Any[0, "state"], Any[1, "customer id"], Any[1, "customer name"], Any[1, "account type"], Any[1, "account balance"], Any[1, "number of loans"], Any[1, "credit score"], Any[1, "branch id"], Any[1, "state"], Any[2, "loan id"], Any[2, "loan type"], Any[2, "customer id"], Any[2, "branch id"], Any[2, "amount"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Customer begin
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
        account_type ~ ChooseUniformly(possibilities[:account_type])
        account_balance ~ ChooseUniformly(possibilities[:account_balance])
        number_of_loans ~ ChooseUniformly(possibilities[:number_of_loans])
        credit_score ~ ChooseUniformly(possibilities[:credit_score])
        branch_id ~ ChooseUniformly(possibilities[:branch_id])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Loan begin
        loan_id ~ ChooseUniformly(possibilities[:loan_id])
        loan_type ~ ChooseUniformly(possibilities[:loan_type])
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        branch_id ~ ChooseUniformly(possibilities[:branch_id])
        amount ~ ChooseUniformly(possibilities[:amount])
    end

    @class Obs begin
        bank ~ Bank
        customer ~ Customer
        loan ~ Loan
    end
end

query = @query Loan1Model.Obs [
    bank_branch_id bank.branch_id
    bank_bname bank.bname
    bank_no_of_customers bank.no_of_customers
    bank_city bank.city
    bank_state bank.state
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
