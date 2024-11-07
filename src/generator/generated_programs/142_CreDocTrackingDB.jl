using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference document types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference document types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location code"], Any[2, "location name"], Any[2, "location description"], Any[3, "role code"], Any[3, "role name"], Any[3, "role description"], Any[4, "document id"], Any[4, "date stored"], Any[4, "document type code"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "role code"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[6, "document id"], Any[6, "location code"], Any[6, "date in location from"], Any[6, "date in locaton to"], Any[7, "document id"], Any[7, "destruction authorised by employee id"], Any[7, "destroyed by employee id"], Any[7, "planned destruction date"], Any[7, "actual destruction date"], Any[7, "other details"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location code"], Any[2, "location name"], Any[2, "location description"], Any[3, "role code"], Any[3, "role name"], Any[3, "role description"], Any[4, "document id"], Any[4, "date stored"], Any[4, "document type code"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "role code"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[6, "document id"], Any[6, "location code"], Any[6, "date in location from"], Any[6, "date in locaton to"], Any[7, "document id"], Any[7, "destruction authorised by employee id"], Any[7, "destroyed by employee id"], Any[7, "planned destruction date"], Any[7, "actual destruction date"], Any[7, "other details"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CreDocTrackingDBModel begin
    @class Reference_Document_Types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_type_name ~ ChooseUniformly(possibilities[:document_type_name])
        document_type_description ~ ChooseUniformly(possibilities[:document_type_description])
    end

    @class Reference_Calendar begin
        calendar_date ~ TimePrior(possibilities[:calendar_date])
        day_number ~ ChooseUniformly(possibilities[:day_number])
    end

    @class Reference_Locations begin
        location_code ~ ChooseUniformly(possibilities[:location_code])
        location_name ~ ChooseUniformly(possibilities[:location_name])
        location_description ~ ChooseUniformly(possibilities[:location_description])
    end

    @class Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_name ~ ChooseUniformly(possibilities[:role_name])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class All_Documents begin
        document_id ~ Unmodeled()
        date_stored ~ TimePrior(possibilities[:date_stored])
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Employees begin
        employee_id ~ Unmodeled()
        role_code ~ ChooseUniformly(possibilities[:role_code])
        employee_name ~ ChooseUniformly(possibilities[:employee_name])
        gender_mfu ~ ChooseUniformly(possibilities[:gender_mfu])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Document_Locations begin
        document_id ~ Unmodeled()
        location_code ~ ChooseUniformly(possibilities[:location_code])
        date_in_location_from ~ TimePrior(possibilities[:date_in_location_from])
        date_in_locaton_to ~ TimePrior(possibilities[:date_in_locaton_to])
    end

    @class Documents_To_Be_Destroyed begin
        document_id ~ Unmodeled()
        destruction_authorised_by_employee_id ~ ChooseUniformly(possibilities[:destruction_authorised_by_employee_id])
        destroyed_by_employee_id ~ ChooseUniformly(possibilities[:destroyed_by_employee_id])
        planned_destruction_date ~ TimePrior(possibilities[:planned_destruction_date])
        actual_destruction_date ~ TimePrior(possibilities[:actual_destruction_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        reference_Document_Types ~ Reference_Document_Types
        reference_Calendar ~ Reference_Calendar
        reference_Locations ~ Reference_Locations
        roles ~ Roles
        all_Documents ~ All_Documents
        employees ~ Employees
        document_Locations ~ Document_Locations
        documents_To_Be_Destroyed ~ Documents_To_Be_Destroyed
    end
end

query = @query CreDocTrackingDBModel.Obs [
    reference_document_types_document_type_code reference_Document_Types.document_type_code
    reference_document_types_document_type_name reference_Document_Types.document_type_name
    reference_document_types_document_type_description reference_Document_Types.document_type_description
    reference_calendar_calendar_date reference_Calendar.calendar_date
    reference_calendar_day_number reference_Calendar.day_number
    reference_locations_location_code reference_Locations.location_code
    reference_locations_location_name reference_Locations.location_name
    reference_locations_location_description reference_Locations.location_description
    roles_role_code roles.role_code
    roles_role_name roles.role_name
    roles_role_description roles.role_description
    all_documents_document_id all_Documents.document_id
    all_documents_document_name all_Documents.document_name
    all_documents_document_description all_Documents.document_description
    all_documents_other_details all_Documents.other_details
    employees_employee_id employees.employee_id
    employees_employee_name employees.employee_name
    employees_gender_mfu employees.gender_mfu
    employees_date_of_birth employees.date_of_birth
    employees_other_details employees.other_details
    documents_to_be_destroyed_other_details documents_To_Be_Destroyed.other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
