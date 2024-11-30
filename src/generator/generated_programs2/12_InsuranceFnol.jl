using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[1, "service id"], Any[1, "service name"], Any[2, "policy id"], Any[2, "policy type code"], Any[2, "customer phone"], Any[3, "customer id"], Any[3, "policy id"], Any[3, "date opened"], Any[3, "date closed"], Any[4, "fnol id"], Any[4, "customer id"], Any[4, "policy id"], Any[4, "service id"], Any[5, "claim id"], Any[5, "fnol id"], Any[5, "effective date"], Any[6, "settlement id"], Any[6, "claim id"], Any[6, "effective date"], Any[6, "settlement amount"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[1, "service id"], Any[1, "service name"], Any[2, "policy id"], Any[2, "policy type code"], Any[2, "customer phone"], Any[3, "customer id"], Any[3, "policy id"], Any[3, "date opened"], Any[3, "date closed"], Any[4, "fnol id"], Any[4, "customer id"], Any[4, "policy id"], Any[4, "service id"], Any[5, "claim id"], Any[5, "fnol id"], Any[5, "effective date"], Any[6, "settlement id"], Any[6, "claim id"], Any[6, "effective date"], Any[6, "settlement amount"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["policy id", "customer id", "customer id", "policy id", "service id", "fnol id", "claim id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "customer name"], Any[1, "service name"], Any[2, "policy type code"], Any[2, "customer phone"], Any[3, "date opened"], Any[3, "date closed"], Any[5, "effective date"], Any[6, "settlement id"], Any[6, "effective date"], Any[6, "settlement amount"]]
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





PClean.@model InsuranceFnolModel begin
    @class Customers begin
        customer_id ~ Unmodeled()
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
    end

    @class Services begin
        service_id ~ Unmodeled()
        service_name ~ ChooseUniformly(possibilities[:service_name])
    end

    @class Available_Policies begin
        policy_id ~ Unmodeled()
        policy_type_code ~ ChooseUniformly(possibilities[:policy_type_code])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
    end

    @class Customers_Policies begin
        customers ~ Customers
        available_Policies ~ Available_Policies
        date_opened ~ TimePrior(possibilities[:date_opened])
        date_closed ~ TimePrior(possibilities[:date_closed])
    end

    @class First_Notification_Of_Loss begin
        fnol_id ~ Unmodeled()
        customers_Policies ~ Customers_Policies
        customers_Policies ~ Customers_Policies
        services ~ Services
    end

    @class Claims begin
        claim_id ~ Unmodeled()
        first_Notification_Of_Loss ~ First_Notification_Of_Loss
        effective_date ~ TimePrior(possibilities[:effective_date])
    end

    @class Settlements begin
        settlement_id ~ Unmodeled()
        claims ~ Claims
        effective_date ~ TimePrior(possibilities[:effective_date])
        settlement_amount ~ ChooseUniformly(possibilities[:settlement_amount])
    end

    @class Obs begin
        settlements ~ Settlements
    end
end

query = @query InsuranceFnolModel.Obs [
    customers_customer_id settlements.claims.first_Notification_Of_Loss.customers_Policies.customers.customer_id
    customers_customer_name settlements.claims.first_Notification_Of_Loss.customers_Policies.customers.customer_name
    services_service_id settlements.claims.first_Notification_Of_Loss.services.service_id
    services_service_name settlements.claims.first_Notification_Of_Loss.services.service_name
    available_policies_policy_id settlements.claims.first_Notification_Of_Loss.customers_Policies.available_Policies.policy_id
    available_policies_policy_type_code settlements.claims.first_Notification_Of_Loss.customers_Policies.available_Policies.policy_type_code
    available_policies_customer_phone settlements.claims.first_Notification_Of_Loss.customers_Policies.available_Policies.customer_phone
    customers_policies_date_opened settlements.claims.first_Notification_Of_Loss.customers_Policies.date_opened
    customers_policies_date_closed settlements.claims.first_Notification_Of_Loss.customers_Policies.date_closed
    first_notification_of_loss_fnol_id settlements.claims.first_Notification_Of_Loss.fnol_id
    claims_claim_id settlements.claims.claim_id
    claims_effective_date settlements.claims.effective_date
    settlements_settlement_id settlements.settlement_id
    settlements_effective_date settlements.effective_date
    settlements_settlement_amount settlements.settlement_amount
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
