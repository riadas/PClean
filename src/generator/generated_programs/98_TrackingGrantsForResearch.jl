using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("document types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("document types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document description"], Any[1, "document id"], Any[1, "document type code"], Any[1, "grant id"], Any[1, "sent date"], Any[1, "response received date"], Any[1, "other details"], Any[2, "grant id"], Any[2, "organisation id"], Any[2, "grant amount"], Any[2, "grant start date"], Any[2, "grant end date"], Any[2, "other details"], Any[3, "organisation type"], Any[3, "organisation type description"], Any[4, "organisation id"], Any[4, "organisation type"], Any[4, "organisation details"], Any[5, "project id"], Any[5, "outcome code"], Any[5, "outcome details"], Any[6, "staff id"], Any[6, "project id"], Any[6, "role code"], Any[6, "date from"], Any[6, "date to"], Any[6, "other details"], Any[7, "project id"], Any[7, "organisation id"], Any[7, "project details"], Any[8, "outcome code"], Any[8, "outcome description"], Any[9, "staff id"], Any[9, "employer organisation id"], Any[9, "staff details"], Any[10, "role code"], Any[10, "role description"], Any[11, "task id"], Any[11, "project id"], Any[11, "task details"], Any[11, "eg agree objectives"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document description"], Any[1, "document id"], Any[1, "document type code"], Any[1, "grant id"], Any[1, "sent date"], Any[1, "response received date"], Any[1, "other details"], Any[2, "grant id"], Any[2, "organisation id"], Any[2, "grant amount"], Any[2, "grant start date"], Any[2, "grant end date"], Any[2, "other details"], Any[3, "organisation type"], Any[3, "organisation type description"], Any[4, "organisation id"], Any[4, "organisation type"], Any[4, "organisation details"], Any[5, "project id"], Any[5, "outcome code"], Any[5, "outcome details"], Any[6, "staff id"], Any[6, "project id"], Any[6, "role code"], Any[6, "date from"], Any[6, "date to"], Any[6, "other details"], Any[7, "project id"], Any[7, "organisation id"], Any[7, "project details"], Any[8, "outcome code"], Any[8, "outcome description"], Any[9, "staff id"], Any[9, "employer organisation id"], Any[9, "staff details"], Any[10, "role code"], Any[10, "role description"], Any[11, "task id"], Any[11, "project id"], Any[11, "task details"], Any[11, "eg agree objectives"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Documents begin
        document_id ~ Unmodeled()
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        grant_id ~ ChooseUniformly(possibilities[:grant_id])
        sent_date ~ TimePrior(possibilities[:sent_date])
        response_received_date ~ TimePrior(possibilities[:response_received_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Grants begin
        grant_id ~ Unmodeled()
        organisation_id ~ ChooseUniformly(possibilities[:organisation_id])
        grant_amount ~ ChooseUniformly(possibilities[:grant_amount])
        grant_start_date ~ TimePrior(possibilities[:grant_start_date])
        grant_end_date ~ TimePrior(possibilities[:grant_end_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Organisation_Types begin
        organisation_type ~ ChooseUniformly(possibilities[:organisation_type])
        organisation_type_description ~ ChooseUniformly(possibilities[:organisation_type_description])
    end

    @class Organisations begin
        organisation_id ~ Unmodeled()
        organisation_type ~ ChooseUniformly(possibilities[:organisation_type])
        organisation_details ~ ChooseUniformly(possibilities[:organisation_details])
    end

    @class Project_Outcomes begin
        project_id ~ Unmodeled()
        outcome_code ~ ChooseUniformly(possibilities[:outcome_code])
        outcome_details ~ ChooseUniformly(possibilities[:outcome_details])
    end

    @class Project_Staff begin
        staff_id ~ Unmodeled()
        project_id ~ ChooseUniformly(possibilities[:project_id])
        role_code ~ ChooseUniformly(possibilities[:role_code])
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Projects begin
        project_id ~ Unmodeled()
        organisation_id ~ ChooseUniformly(possibilities[:organisation_id])
        project_details ~ ChooseUniformly(possibilities[:project_details])
    end

    @class Research_Outcomes begin
        outcome_code ~ ChooseUniformly(possibilities[:outcome_code])
        outcome_description ~ ChooseUniformly(possibilities[:outcome_description])
    end

    @class Research_Staff begin
        staff_id ~ Unmodeled()
        employer_organisation_id ~ ChooseUniformly(possibilities[:employer_organisation_id])
        staff_details ~ ChooseUniformly(possibilities[:staff_details])
    end

    @class Staff_Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class Tasks begin
        task_id ~ Unmodeled()
        project_id ~ ChooseUniformly(possibilities[:project_id])
        task_details ~ ChooseUniformly(possibilities[:task_details])
        eg_agree_objectives ~ ChooseUniformly(possibilities[:eg_agree_objectives])
    end

    @class Obs begin
        document_Types ~ Document_Types
        documents ~ Documents
        grants ~ Grants
        organisation_Types ~ Organisation_Types
        organisations ~ Organisations
        project_Outcomes ~ Project_Outcomes
        project_Staff ~ Project_Staff
        projects ~ Projects
        research_Outcomes ~ Research_Outcomes
        research_Staff ~ Research_Staff
        staff_Roles ~ Staff_Roles
        tasks ~ Tasks
    end
end

query = @query TrackingGrantsForResearchModel.Obs [
    document_types_document_type_code document_Types.document_type_code
    document_types_document_description document_Types.document_description
    documents_document_id documents.document_id
    documents_sent_date documents.sent_date
    documents_response_received_date documents.response_received_date
    documents_other_details documents.other_details
    grants_grant_id grants.grant_id
    grants_grant_amount grants.grant_amount
    grants_grant_start_date grants.grant_start_date
    grants_grant_end_date grants.grant_end_date
    grants_other_details grants.other_details
    organisation_types_organisation_type organisation_Types.organisation_type
    organisation_types_organisation_type_description organisation_Types.organisation_type_description
    organisations_organisation_id organisations.organisation_id
    organisations_organisation_details organisations.organisation_details
    project_outcomes_outcome_details project_Outcomes.outcome_details
    project_staff_staff_id project_Staff.staff_id
    project_staff_date_from project_Staff.date_from
    project_staff_date_to project_Staff.date_to
    project_staff_other_details project_Staff.other_details
    projects_project_id projects.project_id
    projects_project_details projects.project_details
    research_outcomes_outcome_code research_Outcomes.outcome_code
    research_outcomes_outcome_description research_Outcomes.outcome_description
    research_staff_staff_id research_Staff.staff_id
    research_staff_staff_details research_Staff.staff_details
    staff_roles_role_code staff_Roles.role_code
    staff_roles_role_description staff_Roles.role_description
    tasks_task_id tasks.task_id
    tasks_task_details tasks.task_details
    tasks_eg_agree_objectives tasks.eg_agree_objectives
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
