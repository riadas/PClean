using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "policy id"], Any[1, "customer id"], Any[1, "policy type code"], Any[1, "start date"], Any[1, "end date"], Any[2, "claim id"], Any[2, "policy id"], Any[2, "date claim made"], Any[2, "date claim settled"], Any[2, "amount claimed"], Any[2, "amount settled"], Any[3, "settlement id"], Any[3, "claim id"], Any[3, "date claim made"], Any[3, "date claim settled"], Any[3, "amount claimed"], Any[3, "amount settled"], Any[3, "customer policy id"], Any[4, "payment id"], Any[4, "settlement id"], Any[4, "payment method code"], Any[4, "date payment made"], Any[4, "amount payment"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "policy id"], Any[1, "customer id"], Any[1, "policy type code"], Any[1, "start date"], Any[1, "end date"], Any[2, "claim id"], Any[2, "policy id"], Any[2, "date claim made"], Any[2, "date claim settled"], Any[2, "amount claimed"], Any[2, "amount settled"], Any[3, "settlement id"], Any[3, "claim id"], Any[3, "date claim made"], Any[3, "date claim settled"], Any[3, "amount claimed"], Any[3, "amount settled"], Any[3, "customer policy id"], Any[4, "payment id"], Any[4, "settlement id"], Any[4, "payment method code"], Any[4, "date payment made"], Any[4, "amount payment"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Customer_Policies begin
        policy_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        policy_type_code ~ ChooseUniformly(possibilities[:policy_type_code])
        start_date ~ TimePrior(possibilities[:start_date])
        end_date ~ TimePrior(possibilities[:end_date])
    end

    @class Claims begin
        claim_id ~ Unmodeled()
        policy_id ~ ChooseUniformly(possibilities[:policy_id])
        date_claim_made ~ TimePrior(possibilities[:date_claim_made])
        date_claim_settled ~ TimePrior(possibilities[:date_claim_settled])
        amount_claimed ~ ChooseUniformly(possibilities[:amount_claimed])
        amount_settled ~ ChooseUniformly(possibilities[:amount_settled])
    end

    @class Settlements begin
        settlement_id ~ Unmodeled()
        claim_id ~ ChooseUniformly(possibilities[:claim_id])
        date_claim_made ~ TimePrior(possibilities[:date_claim_made])
        date_claim_settled ~ TimePrior(possibilities[:date_claim_settled])
        amount_claimed ~ ChooseUniformly(possibilities[:amount_claimed])
        amount_settled ~ ChooseUniformly(possibilities[:amount_settled])
        customer_policy_id ~ ChooseUniformly(possibilities[:customer_policy_id])
    end

    @class Payments begin
        payment_id ~ Unmodeled()
        settlement_id ~ ChooseUniformly(possibilities[:settlement_id])
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        date_payment_made ~ TimePrior(possibilities[:date_payment_made])
        amount_payment ~ ChooseUniformly(possibilities[:amount_payment])
    end

    @class Obs begin
        customers ~ Customers
        customer_Policies ~ Customer_Policies
        claims ~ Claims
        settlements ~ Settlements
        payments ~ Payments
    end
end

query = @query InsurancePoliciesModel.Obs [
    customers_customer_id customers.customer_id
    customers_customer_details customers.customer_details
    customer_policies_policy_id customer_Policies.policy_id
    customer_policies_policy_type_code customer_Policies.policy_type_code
    customer_policies_start_date customer_Policies.start_date
    customer_policies_end_date customer_Policies.end_date
    claims_claim_id claims.claim_id
    claims_date_claim_made claims.date_claim_made
    claims_date_claim_settled claims.date_claim_settled
    claims_amount_claimed claims.amount_claimed
    claims_amount_settled claims.amount_settled
    settlements_settlement_id settlements.settlement_id
    settlements_date_claim_made settlements.date_claim_made
    settlements_date_claim_settled settlements.date_claim_settled
    settlements_amount_claimed settlements.amount_claimed
    settlements_amount_settled settlements.amount_settled
    settlements_customer_policy_id settlements.customer_policy_id
    payments_payment_id payments.payment_id
    payments_payment_method_code payments.payment_method_code
    payments_date_payment_made payments.date_payment_made
    payments_amount_payment payments.amount_payment
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
