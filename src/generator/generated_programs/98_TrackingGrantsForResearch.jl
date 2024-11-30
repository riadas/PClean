using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("document types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("document types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document description"], Any[1, "document id"], Any[1, "document type code"], Any[1, "grant id"], Any[1, "sent date"], Any[1, "response received date"], Any[1, "other details"], Any[2, "grant id"], Any[2, "organisation id"], Any[2, "grant amount"], Any[2, "grant start date"], Any[2, "grant end date"], Any[2, "other details"], Any[3, "organisation type"], Any[3, "organisation type description"], Any[4, "organisation id"], Any[4, "organisation type"], Any[4, "organisation details"], Any[5, "project id"], Any[5, "outcome code"], Any[5, "outcome details"], Any[6, "staff id"], Any[6, "project id"], Any[6, "role code"], Any[6, "date from"], Any[6, "date to"], Any[6, "other details"], Any[7, "project id"], Any[7, "organisation id"], Any[7, "project details"], Any[8, "outcome code"], Any[8, "outcome description"], Any[9, "staff id"], Any[9, "employer organisation id"], Any[9, "staff details"], Any[10, "role code"], Any[10, "role description"], Any[11, "task id"], Any[11, "project id"], Any[11, "task details"], Any[11, "eg agree objectives"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document description"], Any[1, "document id"], Any[1, "document type code"], Any[1, "grant id"], Any[1, "sent date"], Any[1, "response received date"], Any[1, "other details"], Any[2, "grant id"], Any[2, "organisation id"], Any[2, "grant amount"], Any[2, "grant start date"], Any[2, "grant end date"], Any[2, "other details"], Any[3, "organisation type"], Any[3, "organisation type description"], Any[4, "organisation id"], Any[4, "organisation type"], Any[4, "organisation details"], Any[5, "project id"], Any[5, "outcome code"], Any[5, "outcome details"], Any[6, "staff id"], Any[6, "project id"], Any[6, "role code"], Any[6, "date from"], Any[6, "date to"], Any[6, "other details"], Any[7, "project id"], Any[7, "organisation id"], Any[7, "project details"], Any[8, "outcome code"], Any[8, "outcome description"], Any[9, "staff id"], Any[9, "employer organisation id"], Any[9, "staff details"], Any[10, "role code"], Any[10, "role description"], Any[11, "task id"], Any[11, "project id"], Any[11, "task details"], Any[11, "eg agree objectives"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["grant id", "document type code", "organisation id", "organisation type", "outcome code", "project id", "role code", "project id", "organisation id", "employer organisation id", "project id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "document description"], Any[1, "document id"], Any[1, "sent date"], Any[1, "response received date"], Any[1, "other details"], Any[2, "grant amount"], Any[2, "grant start date"], Any[2, "grant end date"], Any[2, "other details"], Any[3, "organisation type description"], Any[4, "organisation details"], Any[5, "outcome details"], Any[6, "staff id"], Any[6, "date from"], Any[6, "date to"], Any[6, "other details"], Any[7, "project details"], Any[8, "outcome description"], Any[9, "staff id"], Any[9, "staff details"], Any[10, "role description"], Any[11, "task id"], Any[11, "task details"], Any[11, "eg agree objectives"]]
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





PClean.@model TrackingGrantsForResearchModel begin
    @class Document_Types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_description ~ ChooseUniformly(possibilities[:document_description])
    end

    @class Organisation_Types begin
        organisation_type ~ ChooseUniformly(possibilities[:organisation_type])
        organisation_type_description ~ ChooseUniformly(possibilities[:organisation_type_description])
    end

    @class Research_Outcomes begin
        outcome_code ~ ChooseUniformly(possibilities[:outcome_code])
        outcome_description ~ ChooseUniformly(possibilities[:outcome_description])
    end

    @class Staff_Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class Obs begin
        document_Types ~ Document_Types
        organisation_Types ~ Organisation_Types
        research_Outcomes ~ Research_Outcomes
        staff_Roles ~ Staff_Roles
        document_id ~ Unmodeled()
        sent_date ~ TimePrior(possibilities[:sent_date])
        response_received_date ~ TimePrior(possibilities[:response_received_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        grant_id ~ Unmodeled()
        grant_amount ~ ChooseUniformly(possibilities[:grant_amount])
        grant_start_date ~ TimePrior(possibilities[:grant_start_date])
        grant_end_date ~ TimePrior(possibilities[:grant_end_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        organisation_id ~ Unmodeled()
        organisation_details ~ ChooseUniformly(possibilities[:organisation_details])
        outcome_details ~ ChooseUniformly(possibilities[:outcome_details])
        staff_id ~ Unmodeled()
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        project_id ~ Unmodeled()
        project_details ~ ChooseUniformly(possibilities[:project_details])
        staff_id ~ Unmodeled()
        staff_details ~ ChooseUniformly(possibilities[:staff_details])
        task_id ~ Unmodeled()
        task_details ~ ChooseUniformly(possibilities[:task_details])
        eg_agree_objectives ~ ChooseUniformly(possibilities[:eg_agree_objectives])
    end
end

query = @query TrackingGrantsForResearchModel.Obs [
    document_types_document_type_code document_Types.document_type_code
    document_types_document_description document_Types.document_description
    documents_document_id document_id
    documents_sent_date sent_date
    documents_response_received_date response_received_date
    documents_other_details other_details
    grants_grant_id grant_id
    grants_grant_amount grant_amount
    grants_grant_start_date grant_start_date
    grants_grant_end_date grant_end_date
    grants_other_details other_details
    organisation_types_organisation_type organisation_Types.organisation_type
    organisation_types_organisation_type_description organisation_Types.organisation_type_description
    organisations_organisation_id organisation_id
    organisations_organisation_details organisation_details
    project_outcomes_outcome_details outcome_details
    project_staff_staff_id staff_id
    project_staff_date_from date_from
    project_staff_date_to date_to
    project_staff_other_details other_details
    projects_project_id project_id
    projects_project_details project_details
    research_outcomes_outcome_code research_Outcomes.outcome_code
    research_outcomes_outcome_description research_Outcomes.outcome_description
    research_staff_staff_id staff_id
    research_staff_staff_details staff_details
    staff_roles_role_code staff_Roles.role_code
    staff_roles_role_description staff_Roles.role_description
    tasks_task_id task_id
    tasks_task_details task_details
    tasks_eg_agree_objectives eg_agree_objectives
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
