using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference document types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference document types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "budget type code"], Any[1, "budget type description"], Any[2, "project id"], Any[2, "project details"], Any[3, "document id"], Any[3, "document type code"], Any[3, "project id"], Any[3, "document date"], Any[3, "document name"], Any[3, "document description"], Any[3, "other details"], Any[4, "statement id"], Any[4, "statement details"], Any[5, "document id"], Any[5, "budget type code"], Any[5, "document details"], Any[6, "account id"], Any[6, "statement id"], Any[6, "account details"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "budget type code"], Any[1, "budget type description"], Any[2, "project id"], Any[2, "project details"], Any[3, "document id"], Any[3, "document type code"], Any[3, "project id"], Any[3, "document date"], Any[3, "document name"], Any[3, "document description"], Any[3, "other details"], Any[4, "statement id"], Any[4, "statement details"], Any[5, "document id"], Any[5, "budget type code"], Any[5, "document details"], Any[6, "account id"], Any[6, "statement id"], Any[6, "account details"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CreDocsAndEpensesModel begin
    @class Reference_Document_Types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_type_name ~ ChooseUniformly(possibilities[:document_type_name])
        document_type_description ~ ChooseUniformly(possibilities[:document_type_description])
    end

    @class Reference_Budget_Codes begin
        budget_type_code ~ ChooseUniformly(possibilities[:budget_type_code])
        budget_type_description ~ ChooseUniformly(possibilities[:budget_type_description])
    end

    @class Projects begin
        project_id ~ Unmodeled()
        project_details ~ ChooseUniformly(possibilities[:project_details])
    end

    @class Documents begin
        document_id ~ Unmodeled()
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        project_id ~ ChooseUniformly(possibilities[:project_id])
        document_date ~ TimePrior(possibilities[:document_date])
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Statements begin
        statement_id ~ Unmodeled()
        statement_details ~ ChooseUniformly(possibilities[:statement_details])
    end

    @class Documents_With_Expenses begin
        document_id ~ Unmodeled()
        budget_type_code ~ ChooseUniformly(possibilities[:budget_type_code])
        document_details ~ ChooseUniformly(possibilities[:document_details])
    end

    @class Accounts begin
        account_id ~ Unmodeled()
        statement_id ~ ChooseUniformly(possibilities[:statement_id])
        account_details ~ ChooseUniformly(possibilities[:account_details])
    end

    @class Obs begin
        reference_Document_Types ~ Reference_Document_Types
        reference_Budget_Codes ~ Reference_Budget_Codes
        projects ~ Projects
        documents ~ Documents
        statements ~ Statements
        documents_With_Expenses ~ Documents_With_Expenses
        accounts ~ Accounts
    end
end

query = @query CreDocsAndEpensesModel.Obs [
    reference_document_types_document_type_code reference_Document_Types.document_type_code
    reference_document_types_document_type_name reference_Document_Types.document_type_name
    reference_document_types_document_type_description reference_Document_Types.document_type_description
    reference_budget_codes_budget_type_code reference_Budget_Codes.budget_type_code
    reference_budget_codes_budget_type_description reference_Budget_Codes.budget_type_description
    projects_project_id projects.project_id
    projects_project_details projects.project_details
    documents_document_id documents.document_id
    documents_document_date documents.document_date
    documents_document_name documents.document_name
    documents_document_description documents.document_description
    documents_other_details documents.other_details
    statements_statement_details statements.statement_details
    documents_with_expenses_document_details documents_With_Expenses.document_details
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
