using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference_document_types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference_document_types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type description"], Any[1, "role code"], Any[1, "role description"], Any[2, "address id"], Any[2, "address details"], Any[3, "document status code"], Any[3, "document status description"], Any[4, "shipping agent code"], Any[4, "shipping agent name"], Any[4, "shipping agent description"], Any[5, "document id"], Any[5, "document status code"], Any[5, "document type code"], Any[5, "shipping agent code"], Any[5, "receipt date"], Any[5, "receipt number"], Any[5, "other details"], Any[6, "employee id"], Any[6, "role code"], Any[6, "employee name"], Any[6, "other details"], Any[7, "document id"], Any[7, "draft number"], Any[7, "draft details"], Any[8, "document id"], Any[8, "draft number"], Any[8, "copy number"], Any[9, "document id"], Any[9, "draft number"], Any[9, "copy number"], Any[9, "employee id"], Any[10, "document id"], Any[10, "mailed to address id"], Any[10, "mailing date"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type description"], Any[1, "role code"], Any[1, "role description"], Any[2, "address id"], Any[2, "address details"], Any[3, "document status code"], Any[3, "document status description"], Any[4, "shipping agent code"], Any[4, "shipping agent name"], Any[4, "shipping agent description"], Any[5, "document id"], Any[5, "document status code"], Any[5, "document type code"], Any[5, "shipping agent code"], Any[5, "receipt date"], Any[5, "receipt number"], Any[5, "other details"], Any[6, "employee id"], Any[6, "role code"], Any[6, "employee name"], Any[6, "other details"], Any[7, "document id"], Any[7, "draft number"], Any[7, "draft details"], Any[8, "document id"], Any[8, "draft number"], Any[8, "copy number"], Any[9, "document id"], Any[9, "draft number"], Any[9, "copy number"], Any[9, "employee id"], Any[10, "document id"], Any[10, "mailed to address id"], Any[10, "mailing date"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type description"], Any[1, "role code"], Any[1, "role description"], Any[2, "address id"], Any[2, "address details"], Any[3, "document status code"], Any[3, "document status description"], Any[4, "shipping agent code"], Any[4, "shipping agent name"], Any[4, "shipping agent description"], Any[5, "document id"], Any[5, "document status code"], Any[5, "document type code"], Any[5, "shipping agent code"], Any[5, "receipt date"], Any[5, "receipt number"], Any[5, "other details"], Any[6, "employee id"], Any[6, "role code"], Any[6, "employee name"], Any[6, "other details"], Any[7, "document id"], Any[7, "draft number"], Any[7, "draft details"], Any[8, "document id"], Any[8, "draft number"], Any[8, "copy number"], Any[9, "document id"], Any[9, "draft number"], Any[9, "copy number"], Any[9, "employee id"], Any[10, "document id"], Any[10, "mailed to address id"], Any[10, "mailing date"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type description"], Any[1, "role code"], Any[1, "role description"], Any[2, "address id"], Any[2, "address details"], Any[3, "document status code"], Any[3, "document status description"], Any[4, "shipping agent code"], Any[4, "shipping agent name"], Any[4, "shipping agent description"], Any[5, "document id"], Any[5, "document status code"], Any[5, "document type code"], Any[5, "shipping agent code"], Any[5, "receipt date"], Any[5, "receipt number"], Any[5, "other details"], Any[6, "employee id"], Any[6, "role code"], Any[6, "employee name"], Any[6, "other details"], Any[7, "document id"], Any[7, "draft number"], Any[7, "draft details"], Any[8, "document id"], Any[8, "draft number"], Any[8, "copy number"], Any[9, "document id"], Any[9, "draft number"], Any[9, "copy number"], Any[9, "employee id"], Any[10, "document id"], Any[10, "mailed to address id"], Any[10, "mailing date"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type description"], Any[1, "role code"], Any[1, "role description"], Any[2, "address id"], Any[2, "address details"], Any[3, "document status code"], Any[3, "document status description"], Any[4, "shipping agent code"], Any[4, "shipping agent name"], Any[4, "shipping agent description"], Any[5, "document id"], Any[5, "document status code"], Any[5, "document type code"], Any[5, "shipping agent code"], Any[5, "receipt date"], Any[5, "receipt number"], Any[5, "other details"], Any[6, "employee id"], Any[6, "role code"], Any[6, "employee name"], Any[6, "other details"], Any[7, "document id"], Any[7, "draft number"], Any[7, "draft details"], Any[8, "document id"], Any[8, "draft number"], Any[8, "copy number"], Any[9, "document id"], Any[9, "draft number"], Any[9, "copy number"], Any[9, "employee id"], Any[10, "document id"], Any[10, "mailed to address id"], Any[10, "mailing date"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[15, 9], Any[13, 7], Any[14, 1], Any[20, 3], Any[23, 12], Any[26, 23], Any[27, 24], Any[32, 19], Any[29, 26], Any[30, 27], Any[31, 28], Any[34, 5], Any[33, 12]])
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







PClean.@model CreDocControlSystemsModel begin
    @class Reference_document_types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_type_description ~ ChooseUniformly(possibilities[:document_type_description])
    end

    @class Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class Addresses begin
        address_details ~ ChooseUniformly(possibilities[:address_details])
    end

    @class Reference_document_status begin
        document_status_code ~ ChooseUniformly(possibilities[:document_status_code])
        document_status_description ~ ChooseUniformly(possibilities[:document_status_description])
    end

    @class Reference_shipping_agents begin
        shipping_agent_code ~ ChooseUniformly(possibilities[:shipping_agent_code])
        shipping_agent_name ~ ChooseUniformly(possibilities[:shipping_agent_name])
        shipping_agent_description ~ ChooseUniformly(possibilities[:shipping_agent_description])
    end

    @class Documents begin
        reference_document_status ~ Reference_document_status
        reference_document_types ~ Reference_document_types
        reference_shipping_agents ~ Reference_shipping_agents
        receipt_date ~ TimePrior(possibilities[:receipt_date])
        receipt_number ~ ChooseUniformly(possibilities[:receipt_number])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Employees begin
        roles ~ Roles
        employee_name ~ ChooseUniformly(possibilities[:employee_name])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Document_drafts begin
        draft_number ~ ChooseUniformly(possibilities[:draft_number])
        draft_details ~ ChooseUniformly(possibilities[:draft_details])
    end

    @class Draft_copies begin
        document_drafts ~ Document_drafts
        copy_number ~ ChooseUniformly(possibilities[:copy_number])
    end

    @class Circulation_history begin
        draft_copies ~ Draft_copies
        employees ~ Employees
    end

    @class Documents_mailed begin
        addresses ~ Addresses
        mailing_date ~ TimePrior(possibilities[:mailing_date])
    end

    @class Obs begin
        circulation_history ~ Circulation_history
        documents_mailed ~ Documents_mailed
    end
end

query = @query CreDocControlSystemsModel.Obs [
    reference_document_types_document_type_code documents_mailed.documents.reference_document_types.document_type_code
    reference_document_types_document_type_description documents_mailed.documents.reference_document_types.document_type_description
    roles_role_code circulation_history.employees.roles.role_code
    roles_role_description circulation_history.employees.roles.role_description
    addresses_address_id documents_mailed.addresses.address_id
    addresses_address_details documents_mailed.addresses.address_details
    reference_document_status_document_status_code documents_mailed.documents.reference_document_status.document_status_code
    reference_document_status_document_status_description documents_mailed.documents.reference_document_status.document_status_description
    reference_shipping_agents_shipping_agent_code documents_mailed.documents.reference_shipping_agents.shipping_agent_code
    reference_shipping_agents_shipping_agent_name documents_mailed.documents.reference_shipping_agents.shipping_agent_name
    reference_shipping_agents_shipping_agent_description documents_mailed.documents.reference_shipping_agents.shipping_agent_description
    documents_document_id documents_mailed.documents.document_id
    documents_receipt_date documents_mailed.documents.receipt_date
    documents_receipt_number documents_mailed.documents.receipt_number
    documents_other_details documents_mailed.documents.other_details
    employees_employee_id circulation_history.employees.employee_id
    employees_employee_name circulation_history.employees.employee_name
    employees_other_details circulation_history.employees.other_details
    document_drafts_draft_number circulation_history.draft_copies.document_drafts.draft_number
    document_drafts_draft_details circulation_history.draft_copies.document_drafts.draft_details
    draft_copies_copy_number circulation_history.draft_copies.copy_number
    documents_mailed_mailing_date documents_mailed.mailing_date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
