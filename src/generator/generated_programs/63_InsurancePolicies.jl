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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "policy id"], Any[1, "customer id"], Any[1, "policy type code"], Any[1, "start date"], Any[1, "end date"], Any[2, "claim id"], Any[2, "policy id"], Any[2, "date claim made"], Any[2, "date claim settled"], Any[2, "amount claimed"], Any[2, "amount settled"], Any[3, "settlement id"], Any[3, "claim id"], Any[3, "date claim made"], Any[3, "date claim settled"], Any[3, "amount claimed"], Any[3, "amount settled"], Any[3, "customer policy id"], Any[4, "payment id"], Any[4, "settlement id"], Any[4, "payment method code"], Any[4, "date payment made"], Any[4, "amount payment"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "policy id"], Any[1, "customer id"], Any[1, "policy type code"], Any[1, "start date"], Any[1, "end date"], Any[2, "claim id"], Any[2, "policy id"], Any[2, "date claim made"], Any[2, "date claim settled"], Any[2, "amount claimed"], Any[2, "amount settled"], Any[3, "settlement id"], Any[3, "claim id"], Any[3, "date claim made"], Any[3, "date claim settled"], Any[3, "amount claimed"], Any[3, "amount settled"], Any[3, "customer policy id"], Any[4, "payment id"], Any[4, "settlement id"], Any[4, "payment method code"], Any[4, "date payment made"], Any[4, "amount payment"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["customer id", "policy id", "claim id", "settlement id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "customer details"], Any[1, "policy type code"], Any[1, "start date"], Any[1, "end date"], Any[2, "date claim made"], Any[2, "date claim settled"], Any[2, "amount claimed"], Any[2, "amount settled"], Any[3, "date claim made"], Any[3, "date claim settled"], Any[3, "amount claimed"], Any[3, "amount settled"], Any[3, "customer policy id"], Any[4, "payment id"], Any[4, "payment method code"], Any[4, "date payment made"], Any[4, "amount payment"]]
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





PClean.@model InsurancePoliciesModel begin
    @class Customers begin
        customer_id ~ Unmodeled()
        customer_details ~ ChooseUniformly(possibilities[:customer_details])
    end

    @class Obs begin
        customers ~ Customers
        policy_id ~ Unmodeled()
        policy_type_code ~ ChooseUniformly(possibilities[:policy_type_code])
        start_date ~ TimePrior(possibilities[:start_date])
        end_date ~ TimePrior(possibilities[:end_date])
        claim_id ~ Unmodeled()
        date_claim_made ~ TimePrior(possibilities[:date_claim_made])
        date_claim_settled ~ TimePrior(possibilities[:date_claim_settled])
        amount_claimed ~ ChooseUniformly(possibilities[:amount_claimed])
        amount_settled ~ ChooseUniformly(possibilities[:amount_settled])
        settlement_id ~ Unmodeled()
        date_claim_made ~ TimePrior(possibilities[:date_claim_made])
        date_claim_settled ~ TimePrior(possibilities[:date_claim_settled])
        amount_claimed ~ ChooseUniformly(possibilities[:amount_claimed])
        amount_settled ~ ChooseUniformly(possibilities[:amount_settled])
        customer_policy_id ~ ChooseUniformly(possibilities[:customer_policy_id])
        payment_id ~ Unmodeled()
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        date_payment_made ~ TimePrior(possibilities[:date_payment_made])
        amount_payment ~ ChooseUniformly(possibilities[:amount_payment])
    end
end

query = @query InsurancePoliciesModel.Obs [
    customers_customer_id customers.customer_id
    customers_customer_details customers.customer_details
    customer_policies_policy_id policy_id
    customer_policies_policy_type_code policy_type_code
    customer_policies_start_date start_date
    customer_policies_end_date end_date
    claims_claim_id claim_id
    claims_date_claim_made date_claim_made
    claims_date_claim_settled date_claim_settled
    claims_amount_claimed amount_claimed
    claims_amount_settled amount_settled
    settlements_settlement_id settlement_id
    settlements_date_claim_made date_claim_made
    settlements_date_claim_settled date_claim_settled
    settlements_amount_claimed amount_claimed
    settlements_amount_settled amount_settled
    settlements_customer_policy_id customer_policy_id
    payments_payment_id payment_id
    payments_payment_method_code payment_method_code
    payments_date_payment_made date_payment_made
    payments_amount_payment amount_payment
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
