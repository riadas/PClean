using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "staff id"], Any[1, "staff details"], Any[2, "policy id"], Any[2, "customer id"], Any[2, "policy type code"], Any[2, "start date"], Any[2, "end date"], Any[3, "claim header id"], Any[3, "claim status code"], Any[3, "claim type code"], Any[3, "policy id"], Any[3, "date of claim"], Any[3, "date of settlement"], Any[3, "amount claimed"], Any[3, "amount piad"], Any[4, "claim id"], Any[4, "document type code"], Any[4, "created by staff id"], Any[4, "created date"], Any[5, "claim stage id"], Any[5, "next claim stage id"], Any[5, "claim status name"], Any[5, "claim status description"], Any[6, "claim processing id"], Any[6, "claim id"], Any[6, "claim outcome code"], Any[6, "claim stage id"], Any[6, "staff id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "staff id"], Any[1, "staff details"], Any[2, "policy id"], Any[2, "customer id"], Any[2, "policy type code"], Any[2, "start date"], Any[2, "end date"], Any[3, "claim header id"], Any[3, "claim status code"], Any[3, "claim type code"], Any[3, "policy id"], Any[3, "date of claim"], Any[3, "date of settlement"], Any[3, "amount claimed"], Any[3, "amount piad"], Any[4, "claim id"], Any[4, "document type code"], Any[4, "created by staff id"], Any[4, "created date"], Any[5, "claim stage id"], Any[5, "next claim stage id"], Any[5, "claim status name"], Any[5, "claim status description"], Any[6, "claim processing id"], Any[6, "claim id"], Any[6, "claim outcome code"], Any[6, "claim stage id"], Any[6, "staff id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model InsuranceAndEClaimsModel begin
    @class Customers begin
        customer_id ~ Unmodeled()
        customer_details ~ ChooseUniformly(possibilities[:customer_details])
    end

    @class Staff begin
        staff_id ~ Unmodeled()
        staff_details ~ ChooseUniformly(possibilities[:staff_details])
    end

    @class Policies begin
        policy_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        policy_type_code ~ ChooseUniformly(possibilities[:policy_type_code])
        start_date ~ TimePrior(possibilities[:start_date])
        end_date ~ TimePrior(possibilities[:end_date])
    end

    @class Claim_Headers begin
        claim_header_id ~ Unmodeled()
        claim_status_code ~ ChooseUniformly(possibilities[:claim_status_code])
        claim_type_code ~ ChooseUniformly(possibilities[:claim_type_code])
        policy_id ~ ChooseUniformly(possibilities[:policy_id])
        date_of_claim ~ TimePrior(possibilities[:date_of_claim])
        date_of_settlement ~ TimePrior(possibilities[:date_of_settlement])
        amount_claimed ~ ChooseUniformly(possibilities[:amount_claimed])
        amount_piad ~ ChooseUniformly(possibilities[:amount_piad])
    end

    @class Claims_Documents begin
        claim_id ~ Unmodeled()
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        created_by_staff_id ~ ChooseUniformly(possibilities[:created_by_staff_id])
        created_date ~ ChooseUniformly(possibilities[:created_date])
    end

    @class Claims_Processing_Stages begin
        claim_stage_id ~ Unmodeled()
        next_claim_stage_id ~ ChooseUniformly(possibilities[:next_claim_stage_id])
        claim_status_name ~ ChooseUniformly(possibilities[:claim_status_name])
        claim_status_description ~ ChooseUniformly(possibilities[:claim_status_description])
    end

    @class Claims_Processing begin
        claim_processing_id ~ Unmodeled()
        claim_id ~ ChooseUniformly(possibilities[:claim_id])
        claim_outcome_code ~ ChooseUniformly(possibilities[:claim_outcome_code])
        claim_stage_id ~ ChooseUniformly(possibilities[:claim_stage_id])
        staff_id ~ ChooseUniformly(possibilities[:staff_id])
    end

    @class Obs begin
        customers ~ Customers
        staff ~ Staff
        policies ~ Policies
        claim_Headers ~ Claim_Headers
        claims_Documents ~ Claims_Documents
        claims_Processing_Stages ~ Claims_Processing_Stages
        claims_Processing ~ Claims_Processing
    end
end

query = @query InsuranceAndEClaimsModel.Obs [
    customers_customer_id customers.customer_id
    customers_customer_details customers.customer_details
    staff_id staff.staff_id
    staff_details staff.staff_details
    policies_policy_id policies.policy_id
    policies_policy_type_code policies.policy_type_code
    policies_start_date policies.start_date
    policies_end_date policies.end_date
    claim_headers_claim_header_id claim_Headers.claim_header_id
    claim_headers_claim_status_code claim_Headers.claim_status_code
    claim_headers_claim_type_code claim_Headers.claim_type_code
    claim_headers_date_of_claim claim_Headers.date_of_claim
    claim_headers_date_of_settlement claim_Headers.date_of_settlement
    claim_headers_amount_claimed claim_Headers.amount_claimed
    claim_headers_amount_piad claim_Headers.amount_piad
    claims_documents_document_type_code claims_Documents.document_type_code
    claims_documents_created_date claims_Documents.created_date
    claims_processing_stages_claim_stage_id claims_Processing_Stages.claim_stage_id
    claims_processing_stages_next_claim_stage_id claims_Processing_Stages.next_claim_stage_id
    claims_processing_stages_claim_status_name claims_Processing_Stages.claim_status_name
    claims_processing_stages_claim_status_description claims_Processing_Stages.claim_status_description
    claims_processing_claim_processing_id claims_Processing.claim_processing_id
    claims_processing_claim_outcome_code claims_Processing.claim_outcome_code
    claims_processing_claim_stage_id claims_Processing.claim_stage_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
