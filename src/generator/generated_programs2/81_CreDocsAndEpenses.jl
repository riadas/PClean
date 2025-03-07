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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "budget type code"], Any[1, "budget type description"], Any[2, "project id"], Any[2, "project details"], Any[3, "document id"], Any[3, "document type code"], Any[3, "project id"], Any[3, "document date"], Any[3, "document name"], Any[3, "document description"], Any[3, "other details"], Any[4, "statement id"], Any[4, "statement details"], Any[5, "document id"], Any[5, "budget type code"], Any[5, "document details"], Any[6, "account id"], Any[6, "statement id"], Any[6, "account details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "budget type code"], Any[1, "budget type description"], Any[2, "project id"], Any[2, "project details"], Any[3, "document id"], Any[3, "document type code"], Any[3, "project id"], Any[3, "document date"], Any[3, "document name"], Any[3, "document description"], Any[3, "other details"], Any[4, "statement id"], Any[4, "statement details"], Any[5, "document id"], Any[5, "budget type code"], Any[5, "document details"], Any[6, "account id"], Any[6, "statement id"], Any[6, "account details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "budget type code"], Any[1, "budget type description"], Any[2, "project id"], Any[2, "project details"], Any[3, "document id"], Any[3, "document type code"], Any[3, "project id"], Any[3, "document date"], Any[3, "document name"], Any[3, "document description"], Any[3, "other details"], Any[4, "statement id"], Any[4, "statement details"], Any[5, "document id"], Any[5, "budget type code"], Any[5, "document details"], Any[6, "account id"], Any[6, "statement id"], Any[6, "account details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "budget type code"], Any[1, "budget type description"], Any[2, "project id"], Any[2, "project details"], Any[3, "document id"], Any[3, "document type code"], Any[3, "project id"], Any[3, "document date"], Any[3, "document name"], Any[3, "document description"], Any[3, "other details"], Any[4, "statement id"], Any[4, "statement details"], Any[5, "document id"], Any[5, "budget type code"], Any[5, "document details"], Any[6, "account id"], Any[6, "statement id"], Any[6, "account details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "budget type code"], Any[1, "budget type description"], Any[2, "project id"], Any[2, "project details"], Any[3, "document id"], Any[3, "document type code"], Any[3, "project id"], Any[3, "document date"], Any[3, "document name"], Any[3, "document description"], Any[3, "other details"], Any[4, "statement id"], Any[4, "statement details"], Any[5, "document id"], Any[5, "budget type code"], Any[5, "document details"], Any[6, "account id"], Any[6, "statement id"], Any[6, "account details"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[10, 6], Any[9, 1], Any[15, 8], Any[17, 8], Any[18, 4], Any[21, 15]])
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







PClean.@model CreDocsAndEpensesModel begin
    @class Reference_document_types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_type_name ~ ChooseUniformly(possibilities[:document_type_name])
        document_type_description ~ ChooseUniformly(possibilities[:document_type_description])
    end

    @class Reference_budget_codes begin
        budget_type_code ~ ChooseUniformly(possibilities[:budget_type_code])
        budget_type_description ~ ChooseUniformly(possibilities[:budget_type_description])
    end

    @class Projects begin
        project_details ~ ChooseUniformly(possibilities[:project_details])
    end

    @class Documents begin
        reference_document_types ~ Reference_document_types
        projects ~ Projects
        document_date ~ TimePrior(possibilities[:document_date])
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Statements begin
        statement_details ~ ChooseUniformly(possibilities[:statement_details])
    end

    @class Documents_with_expenses begin
        reference_budget_codes ~ Reference_budget_codes
        document_details ~ ChooseUniformly(possibilities[:document_details])
    end

    @class Accounts begin
        statements ~ Statements
        account_details ~ ChooseUniformly(possibilities[:account_details])
    end

    @class Obs begin
        documents_with_expenses ~ Documents_with_expenses
        accounts ~ Accounts
    end
end

query = @query CreDocsAndEpensesModel.Obs [
    reference_document_types_document_type_code documents_with_expenses.documents.reference_document_types.document_type_code
    reference_document_types_document_type_name documents_with_expenses.documents.reference_document_types.document_type_name
    reference_document_types_document_type_description documents_with_expenses.documents.reference_document_types.document_type_description
    reference_budget_codes_budget_type_code documents_with_expenses.reference_budget_codes.budget_type_code
    reference_budget_codes_budget_type_description documents_with_expenses.reference_budget_codes.budget_type_description
    projects_project_id documents_with_expenses.documents.projects.project_id
    projects_project_details documents_with_expenses.documents.projects.project_details
    documents_document_id documents_with_expenses.documents.document_id
    documents_document_date documents_with_expenses.documents.document_date
    documents_document_name documents_with_expenses.documents.document_name
    documents_document_description documents_with_expenses.documents.document_description
    documents_other_details documents_with_expenses.documents.other_details
    statements_statement_details accounts.statements.statement_details
    documents_with_expenses_document_details documents_with_expenses.document_details
    accounts_account_id accounts.account_id
    accounts_account_details accounts.account_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
