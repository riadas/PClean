using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "policy id"], Any[1, "customer id"], Any[1, "policy type code"], Any[1, "start date"], Any[1, "end date"], Any[2, "claim id"], Any[2, "policy id"], Any[2, "date claim made"], Any[2, "date claim settled"], Any[2, "amount claimed"], Any[2, "amount settled"], Any[3, "settlement id"], Any[3, "claim id"], Any[3, "date claim made"], Any[3, "date claim settled"], Any[3, "amount claimed"], Any[3, "amount settled"], Any[3, "customer policy id"], Any[4, "payment id"], Any[4, "settlement id"], Any[4, "payment method code"], Any[4, "date payment made"], Any[4, "amount payment"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[4, 1], Any[9, 3], Any[15, 8], Any[22, 14]])
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







PClean.@model InsurancePoliciesModel begin
    @class Customers begin
        customer_details ~ ChooseUniformly(possibilities[:customer_details])
    end

    @class Customer_policies begin
        customers ~ Customers
        policy_type_code ~ ChooseUniformly(possibilities[:policy_type_code])
        start_date ~ TimePrior(possibilities[:start_date])
        end_date ~ TimePrior(possibilities[:end_date])
    end

    @class Claims begin
        customer_policies ~ Customer_policies
        date_claim_made ~ TimePrior(possibilities[:date_claim_made])
        date_claim_settled ~ TimePrior(possibilities[:date_claim_settled])
        amount_claimed ~ ChooseUniformly(possibilities[:amount_claimed])
        amount_settled ~ ChooseUniformly(possibilities[:amount_settled])
    end

    @class Settlements begin
        claims ~ Claims
        date_claim_made ~ TimePrior(possibilities[:date_claim_made])
        date_claim_settled ~ TimePrior(possibilities[:date_claim_settled])
        amount_claimed ~ ChooseUniformly(possibilities[:amount_claimed])
        amount_settled ~ ChooseUniformly(possibilities[:amount_settled])
        customer_policy_id ~ ChooseUniformly(possibilities[:customer_policy_id])
    end

    @class Payments begin
        settlements ~ Settlements
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        date_payment_made ~ TimePrior(possibilities[:date_payment_made])
        amount_payment ~ ChooseUniformly(possibilities[:amount_payment])
    end

    @class Obs begin
        payments ~ Payments
    end
end

query = @query InsurancePoliciesModel.Obs [
    customers_customer_id payments.settlements.claims.customer_policies.customers.customer_id
    customers_customer_details payments.settlements.claims.customer_policies.customers.customer_details
    customer_policies_policy_id payments.settlements.claims.customer_policies.policy_id
    customer_policies_policy_type_code payments.settlements.claims.customer_policies.policy_type_code
    customer_policies_start_date payments.settlements.claims.customer_policies.start_date
    customer_policies_end_date payments.settlements.claims.customer_policies.end_date
    claims_claim_id payments.settlements.claims.claim_id
    claims_date_claim_made payments.settlements.claims.date_claim_made
    claims_date_claim_settled payments.settlements.claims.date_claim_settled
    claims_amount_claimed payments.settlements.claims.amount_claimed
    claims_amount_settled payments.settlements.claims.amount_settled
    settlements_settlement_id payments.settlements.settlement_id
    settlements_date_claim_made payments.settlements.date_claim_made
    settlements_date_claim_settled payments.settlements.date_claim_settled
    settlements_amount_claimed payments.settlements.amount_claimed
    settlements_amount_settled payments.settlements.amount_settled
    settlements_customer_policy_id payments.settlements.customer_policy_id
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
