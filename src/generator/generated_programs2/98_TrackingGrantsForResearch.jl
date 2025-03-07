using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("document_types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("document_types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document description"], Any[1, "document id"], Any[1, "document type code"], Any[1, "grant id"], Any[1, "sent date"], Any[1, "response received date"], Any[1, "other details"], Any[2, "grant id"], Any[2, "organisation id"], Any[2, "grant amount"], Any[2, "grant start date"], Any[2, "grant end date"], Any[2, "other details"], Any[3, "organisation type"], Any[3, "organisation type description"], Any[4, "organisation id"], Any[4, "organisation type"], Any[4, "organisation details"], Any[5, "project id"], Any[5, "outcome code"], Any[5, "outcome details"], Any[6, "staff id"], Any[6, "project id"], Any[6, "role code"], Any[6, "date from"], Any[6, "date to"], Any[6, "other details"], Any[7, "project id"], Any[7, "organisation id"], Any[7, "project details"], Any[8, "outcome code"], Any[8, "outcome description"], Any[9, "staff id"], Any[9, "employer organisation id"], Any[9, "staff details"], Any[10, "role code"], Any[10, "role description"], Any[11, "task id"], Any[11, "project id"], Any[11, "task details"], Any[11, "eg agree objectives"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 9], Any[4, 1], Any[10, 17], Any[18, 15], Any[21, 32], Any[20, 29], Any[25, 37], Any[24, 29], Any[30, 17], Any[35, 17], Any[40, 29]])
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







PClean.@model TrackingGrantsForResearchModel begin
    @class Document_types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_description ~ ChooseUniformly(possibilities[:document_description])
    end

    @class Organisation_types begin
        organisation_type ~ ChooseUniformly(possibilities[:organisation_type])
        organisation_type_description ~ ChooseUniformly(possibilities[:organisation_type_description])
    end

    @class Organisations begin
        organisation_types ~ Organisation_types
        organisation_details ~ ChooseUniformly(possibilities[:organisation_details])
    end

    @class Projects begin
        organisations ~ Organisations
        project_details ~ ChooseUniformly(possibilities[:project_details])
    end

    @class Research_outcomes begin
        outcome_code ~ ChooseUniformly(possibilities[:outcome_code])
        outcome_description ~ ChooseUniformly(possibilities[:outcome_description])
    end

    @class Research_staff begin
        organisations ~ Organisations
        staff_details ~ ChooseUniformly(possibilities[:staff_details])
    end

    @class Staff_roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class Tasks begin
        projects ~ Projects
        task_details ~ ChooseUniformly(possibilities[:task_details])
        eg_agree_objectives ~ ChooseUniformly(possibilities[:eg_agree_objectives])
    end

    @class Grants begin
        organisations ~ Organisations
        grant_amount ~ ChooseUniformly(possibilities[:grant_amount])
        grant_start_date ~ TimePrior(possibilities[:grant_start_date])
        grant_end_date ~ TimePrior(possibilities[:grant_end_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Project_outcomes begin
        projects ~ Projects
        research_outcomes ~ Research_outcomes
        outcome_details ~ ChooseUniformly(possibilities[:outcome_details])
    end

    @class Project_staff begin
        projects ~ Projects
        staff_roles ~ Staff_roles
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Documents begin
        document_types ~ Document_types
        grants ~ Grants
        sent_date ~ TimePrior(possibilities[:sent_date])
        response_received_date ~ TimePrior(possibilities[:response_received_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        documents ~ Documents
        project_outcomes ~ Project_outcomes
        project_staff ~ Project_staff
        research_staff ~ Research_staff
        tasks ~ Tasks
    end
end

query = @query TrackingGrantsForResearchModel.Obs [
    document_types_document_type_code documents.document_types.document_type_code
    document_types_document_description documents.document_types.document_description
    documents_document_id documents.document_id
    documents_sent_date documents.sent_date
    documents_response_received_date documents.response_received_date
    documents_other_details documents.other_details
    grants_grant_id documents.grants.grant_id
    grants_grant_amount documents.grants.grant_amount
    grants_grant_start_date documents.grants.grant_start_date
    grants_grant_end_date documents.grants.grant_end_date
    grants_other_details documents.grants.other_details
    organisation_types_organisation_type research_staff.organisations.organisation_types.organisation_type
    organisation_types_organisation_type_description research_staff.organisations.organisation_types.organisation_type_description
    organisations_organisation_id research_staff.organisations.organisation_id
    organisations_organisation_details research_staff.organisations.organisation_details
    project_outcomes_outcome_details project_outcomes.outcome_details
    project_staff_staff_id project_staff.staff_id
    project_staff_date_from project_staff.date_from
    project_staff_date_to project_staff.date_to
    project_staff_other_details project_staff.other_details
    projects_project_id project_outcomes.projects.project_id
    projects_project_details project_outcomes.projects.project_details
    research_outcomes_outcome_code project_outcomes.research_outcomes.outcome_code
    research_outcomes_outcome_description project_outcomes.research_outcomes.outcome_description
    research_staff_staff_id research_staff.staff_id
    research_staff_staff_details research_staff.staff_details
    staff_roles_role_code project_staff.staff_roles.role_code
    staff_roles_role_description project_staff.staff_roles.role_description
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
