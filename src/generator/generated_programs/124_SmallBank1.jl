using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("accounts_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("accounts_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "name"], Any[1, "customer id"], Any[1, "balance"], Any[2, "customer id"], Any[2, "balance"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "name"], Any[1, "customer id"], Any[1, "balance"], Any[2, "customer id"], Any[2, "balance"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model SmallBank1Model begin
    @class Accounts begin
        customer_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Savings begin
        customer_id ~ Unmodeled()
        balance ~ ChooseUniformly(possibilities[:balance])
    end

    @class Checking begin
        customer_id ~ Unmodeled()
        balance ~ ChooseUniformly(possibilities[:balance])
    end

    @class Obs begin
        accounts ~ Accounts
        savings ~ Savings
        checking ~ Checking
    end
end

query = @query SmallBank1Model.Obs [
    accounts_customer_id accounts.customer_id
    accounts_name accounts.name
    savings_balance savings.balance
    checking_balance checking.balance
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
