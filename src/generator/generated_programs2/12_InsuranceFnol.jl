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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[1, "service id"], Any[1, "service name"], Any[2, "policy id"], Any[2, "policy type code"], Any[2, "customer phone"], Any[3, "customer id"], Any[3, "policy id"], Any[3, "date opened"], Any[3, "date closed"], Any[4, "fnol id"], Any[4, "customer id"], Any[4, "policy id"], Any[4, "service id"], Any[5, "claim id"], Any[5, "fnol id"], Any[5, "effective date"], Any[6, "settlement id"], Any[6, "claim id"], Any[6, "effective date"], Any[6, "settlement amount"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[1, "service id"], Any[1, "service name"], Any[2, "policy id"], Any[2, "policy type code"], Any[2, "customer phone"], Any[3, "customer id"], Any[3, "policy id"], Any[3, "date opened"], Any[3, "date closed"], Any[4, "fnol id"], Any[4, "customer id"], Any[4, "policy id"], Any[4, "service id"], Any[5, "claim id"], Any[5, "fnol id"], Any[5, "effective date"], Any[6, "settlement id"], Any[6, "claim id"], Any[6, "effective date"], Any[6, "settlement amount"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
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
cols = Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer name"], Any[1, "service id"], Any[1, "service name"], Any[2, "policy id"], Any[2, "policy type code"], Any[2, "customer phone"], Any[3, "customer id"], Any[3, "policy id"], Any[3, "date opened"], Any[3, "date closed"], Any[4, "fnol id"], Any[4, "customer id"], Any[4, "policy id"], Any[4, "service id"], Any[5, "claim id"], Any[5, "fnol id"], Any[5, "effective date"], Any[6, "settlement id"], Any[6, "claim id"], Any[6, "effective date"], Any[6, "settlement amount"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 5], Any[8, 1], Any[13, 8], Any[14, 9], Any[15, 3], Any[17, 12], Any[20, 16]])
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







PClean.@model InsuranceFnolModel begin
    @class Customers begin
        customer_name ~ ChooseUniformly(possibilities[:customer_name])
    end

    @class Services begin
        service_name ~ ChooseUniformly(possibilities[:service_name])
    end

    @class Available_policies begin
        policy_type_code ~ ChooseUniformly(possibilities[:policy_type_code])
        customer_phone ~ ChooseUniformly(possibilities[:customer_phone])
    end

    @class Customers_policies begin
        available_policies ~ Available_policies
        date_opened ~ TimePrior(possibilities[:date_opened])
        date_closed ~ TimePrior(possibilities[:date_closed])
    end

    @class First_notification_of_loss begin
        customers_policies ~ Customers_policies
        services ~ Services
    end

    @class Claims begin
        first_notification_of_loss ~ First_notification_of_loss
        effective_date ~ TimePrior(possibilities[:effective_date])
    end

    @class Settlements begin
        claims ~ Claims
        effective_date ~ TimePrior(possibilities[:effective_date])
        settlement_amount ~ ChooseUniformly(possibilities[:settlement_amount])
    end

    @class Obs begin
        settlements ~ Settlements
    end
end

query = @query InsuranceFnolModel.Obs [
    customers_customer_id settlements.claims.first_notification_of_loss.customers_policies.customers.customer_id
    customers_customer_name settlements.claims.first_notification_of_loss.customers_policies.customers.customer_name
    services_service_id settlements.claims.first_notification_of_loss.services.service_id
    services_service_name settlements.claims.first_notification_of_loss.services.service_name
    available_policies_policy_id settlements.claims.first_notification_of_loss.customers_policies.available_policies.policy_id
    available_policies_policy_type_code settlements.claims.first_notification_of_loss.customers_policies.available_policies.policy_type_code
    available_policies_customer_phone settlements.claims.first_notification_of_loss.customers_policies.available_policies.customer_phone
    customers_policies_date_opened settlements.claims.first_notification_of_loss.customers_policies.date_opened
    customers_policies_date_closed settlements.claims.first_notification_of_loss.customers_policies.date_closed
    first_notification_of_loss_fnol_id settlements.claims.first_notification_of_loss.fnol_id
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
