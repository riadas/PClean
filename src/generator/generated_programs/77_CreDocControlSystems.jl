using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference document types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference document types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type description"], Any[1, "role code"], Any[1, "role description"], Any[2, "address id"], Any[2, "address details"], Any[3, "document status code"], Any[3, "document status description"], Any[4, "shipping agent code"], Any[4, "shipping agent name"], Any[4, "shipping agent description"], Any[5, "document id"], Any[5, "document status code"], Any[5, "document type code"], Any[5, "shipping agent code"], Any[5, "receipt date"], Any[5, "receipt number"], Any[5, "other details"], Any[6, "employee id"], Any[6, "role code"], Any[6, "employee name"], Any[6, "other details"], Any[7, "document id"], Any[7, "draft number"], Any[7, "draft details"], Any[8, "document id"], Any[8, "draft number"], Any[8, "copy number"], Any[9, "document id"], Any[9, "draft number"], Any[9, "copy number"], Any[9, "employee id"], Any[10, "document id"], Any[10, "mailed to address id"], Any[10, "mailing date"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type description"], Any[1, "role code"], Any[1, "role description"], Any[2, "address id"], Any[2, "address details"], Any[3, "document status code"], Any[3, "document status description"], Any[4, "shipping agent code"], Any[4, "shipping agent name"], Any[4, "shipping agent description"], Any[5, "document id"], Any[5, "document status code"], Any[5, "document type code"], Any[5, "shipping agent code"], Any[5, "receipt date"], Any[5, "receipt number"], Any[5, "other details"], Any[6, "employee id"], Any[6, "role code"], Any[6, "employee name"], Any[6, "other details"], Any[7, "document id"], Any[7, "draft number"], Any[7, "draft details"], Any[8, "document id"], Any[8, "draft number"], Any[8, "copy number"], Any[9, "document id"], Any[9, "draft number"], Any[9, "copy number"], Any[9, "employee id"], Any[10, "document id"], Any[10, "mailed to address id"], Any[10, "mailing date"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CreDocControlSystemsModel begin
    @class Reference_Document_Types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_type_description ~ ChooseUniformly(possibilities[:document_type_description])
    end

    @class Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class Addresses begin
        address_id ~ Unmodeled()
        address_details ~ ChooseUniformly(possibilities[:address_details])
    end

    @class Reference_Document_Status begin
        document_status_code ~ ChooseUniformly(possibilities[:document_status_code])
        document_status_description ~ ChooseUniformly(possibilities[:document_status_description])
    end

    @class Reference_Shipping_Agents begin
        shipping_agent_code ~ ChooseUniformly(possibilities[:shipping_agent_code])
        shipping_agent_name ~ ChooseUniformly(possibilities[:shipping_agent_name])
        shipping_agent_description ~ ChooseUniformly(possibilities[:shipping_agent_description])
    end

    @class Documents begin
        document_id ~ Unmodeled()
        document_status_code ~ ChooseUniformly(possibilities[:document_status_code])
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        shipping_agent_code ~ ChooseUniformly(possibilities[:shipping_agent_code])
        receipt_date ~ TimePrior(possibilities[:receipt_date])
        receipt_number ~ ChooseUniformly(possibilities[:receipt_number])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Employees begin
        employee_id ~ Unmodeled()
        role_code ~ ChooseUniformly(possibilities[:role_code])
        employee_name ~ ChooseUniformly(possibilities[:employee_name])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Document_Drafts begin
        document_id ~ Unmodeled()
        draft_number ~ ChooseUniformly(possibilities[:draft_number])
        draft_details ~ ChooseUniformly(possibilities[:draft_details])
    end

    @class Draft_Copies begin
        document_id ~ Unmodeled()
        draft_number ~ ChooseUniformly(possibilities[:draft_number])
        copy_number ~ ChooseUniformly(possibilities[:copy_number])
    end

    @class Circulation_History begin
        document_id ~ Unmodeled()
        draft_number ~ ChooseUniformly(possibilities[:draft_number])
        copy_number ~ ChooseUniformly(possibilities[:copy_number])
        employee_id ~ ChooseUniformly(possibilities[:employee_id])
    end

    @class Documents_Mailed begin
        document_id ~ Unmodeled()
        mailed_to_address_id ~ ChooseUniformly(possibilities[:mailed_to_address_id])
        mailing_date ~ TimePrior(possibilities[:mailing_date])
    end

    @class Obs begin
        reference_Document_Types ~ Reference_Document_Types
        roles ~ Roles
        addresses ~ Addresses
        reference_Document_Status ~ Reference_Document_Status
        reference_Shipping_Agents ~ Reference_Shipping_Agents
        documents ~ Documents
        employees ~ Employees
        document_Drafts ~ Document_Drafts
        draft_Copies ~ Draft_Copies
        circulation_History ~ Circulation_History
        documents_Mailed ~ Documents_Mailed
    end
end

query = @query CreDocControlSystemsModel.Obs [
    reference_document_types_document_type_code reference_Document_Types.document_type_code
    reference_document_types_document_type_description reference_Document_Types.document_type_description
    roles_role_code roles.role_code
    roles_role_description roles.role_description
    addresses_address_id addresses.address_id
    addresses_address_details addresses.address_details
    reference_document_status_document_status_code reference_Document_Status.document_status_code
    reference_document_status_document_status_description reference_Document_Status.document_status_description
    reference_shipping_agents_shipping_agent_code reference_Shipping_Agents.shipping_agent_code
    reference_shipping_agents_shipping_agent_name reference_Shipping_Agents.shipping_agent_name
    reference_shipping_agents_shipping_agent_description reference_Shipping_Agents.shipping_agent_description
    documents_document_id documents.document_id
    documents_receipt_date documents.receipt_date
    documents_receipt_number documents.receipt_number
    documents_other_details documents.other_details
    employees_employee_id employees.employee_id
    employees_employee_name employees.employee_name
    employees_other_details employees.other_details
    document_drafts_draft_number document_Drafts.draft_number
    document_drafts_draft_details document_Drafts.draft_details
    draft_copies_copy_number draft_Copies.copy_number
    documents_mailed_mailing_date documents_Mailed.mailing_date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
