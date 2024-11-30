using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference document types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference document types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location code"], Any[2, "location name"], Any[2, "location description"], Any[3, "role code"], Any[3, "role name"], Any[3, "role description"], Any[4, "document id"], Any[4, "date stored"], Any[4, "document type code"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "role code"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[6, "document id"], Any[6, "location code"], Any[6, "date in location from"], Any[6, "date in locaton to"], Any[7, "document id"], Any[7, "destruction authorised by employee id"], Any[7, "destroyed by employee id"], Any[7, "planned destruction date"], Any[7, "actual destruction date"], Any[7, "other details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location code"], Any[2, "location name"], Any[2, "location description"], Any[3, "role code"], Any[3, "role name"], Any[3, "role description"], Any[4, "document id"], Any[4, "date stored"], Any[4, "document type code"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "role code"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[6, "document id"], Any[6, "location code"], Any[6, "date in location from"], Any[6, "date in locaton to"], Any[7, "document id"], Any[7, "destruction authorised by employee id"], Any[7, "destroyed by employee id"], Any[7, "planned destruction date"], Any[7, "actual destruction date"], Any[7, "other details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["date stored", "document type code", "role code", "document id", "date in locaton to", "date in location from", "location code", "document id", "actual destruction date", "planned destruction date", "destruction authorised by employee id", "destroyed by employee id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location name"], Any[2, "location description"], Any[3, "role name"], Any[3, "role description"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[7, "other details"]]
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

    @class Obs begin
        reference_Document_Types ~ Reference_Document_Types
        reference_Calendar ~ Reference_Calendar
        reference_Locations ~ Reference_Locations
        roles ~ Roles
        document_id ~ Unmodeled()
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        employee_id ~ Unmodeled()
        employee_name ~ ChooseUniformly(possibilities[:employee_name])
        gender_mfu ~ ChooseUniformly(possibilities[:gender_mfu])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        other_details ~ ChooseUniformly(possibilities[:other_details])
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
    all_documents_document_id document_id
    all_documents_document_name document_name
    all_documents_document_description document_description
    all_documents_other_details other_details
    employees_employee_id employee_id
    employees_employee_name employee_name
    employees_gender_mfu gender_mfu
    employees_date_of_birth date_of_birth
    employees_other_details other_details
    documents_to_be_destroyed_other_details other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
