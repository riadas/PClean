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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location code"], Any[2, "location name"], Any[2, "location description"], Any[3, "role code"], Any[3, "role name"], Any[3, "role description"], Any[4, "document id"], Any[4, "date stored"], Any[4, "document type code"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "role code"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[6, "document id"], Any[6, "location code"], Any[6, "date in location from"], Any[6, "date in locaton to"], Any[7, "document id"], Any[7, "destruction authorised by employee id"], Any[7, "destroyed by employee id"], Any[7, "planned destruction date"], Any[7, "actual destruction date"], Any[7, "other details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location code"], Any[2, "location name"], Any[2, "location description"], Any[3, "role code"], Any[3, "role name"], Any[3, "role description"], Any[4, "document id"], Any[4, "date stored"], Any[4, "document type code"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "role code"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[6, "document id"], Any[6, "location code"], Any[6, "date in location from"], Any[6, "date in locaton to"], Any[7, "document id"], Any[7, "destruction authorised by employee id"], Any[7, "destroyed by employee id"], Any[7, "planned destruction date"], Any[7, "actual destruction date"], Any[7, "other details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
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
cols = Any[Any[-1, "*"], Any[0, "document type code"], Any[0, "document type name"], Any[0, "document type description"], Any[1, "calendar date"], Any[1, "day number"], Any[2, "location code"], Any[2, "location name"], Any[2, "location description"], Any[3, "role code"], Any[3, "role name"], Any[3, "role description"], Any[4, "document id"], Any[4, "date stored"], Any[4, "document type code"], Any[4, "document name"], Any[4, "document description"], Any[4, "other details"], Any[5, "employee id"], Any[5, "role code"], Any[5, "employee name"], Any[5, "gender mfu"], Any[5, "date of birth"], Any[5, "other details"], Any[6, "document id"], Any[6, "location code"], Any[6, "date in location from"], Any[6, "date in locaton to"], Any[7, "document id"], Any[7, "destruction authorised by employee id"], Any[7, "destroyed by employee id"], Any[7, "planned destruction date"], Any[7, "actual destruction date"], Any[7, "other details"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 4], Any[14, 1], Any[19, 9], Any[24, 12], Any[27, 4], Any[26, 4], Any[25, 6], Any[28, 12], Any[32, 4], Any[31, 4], Any[29, 18], Any[30, 18]])
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







PClean.@model CreDocTrackingDBModel begin
    @class Reference_document_types begin
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        document_type_name ~ ChooseUniformly(possibilities[:document_type_name])
        document_type_description ~ ChooseUniformly(possibilities[:document_type_description])
    end

    @class Reference_calendar begin
        calendar_date ~ TimePrior(possibilities[:calendar_date])
        day_number ~ ChooseUniformly(possibilities[:day_number])
    end

    @class Reference_locations begin
        location_code ~ ChooseUniformly(possibilities[:location_code])
        location_name ~ ChooseUniformly(possibilities[:location_name])
        location_description ~ ChooseUniformly(possibilities[:location_description])
    end

    @class Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_name ~ ChooseUniformly(possibilities[:role_name])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class All_documents begin
        reference_calendar ~ Reference_calendar
        reference_document_types ~ Reference_document_types
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Employees begin
        roles ~ Roles
        employee_name ~ ChooseUniformly(possibilities[:employee_name])
        gender_mfu ~ ChooseUniformly(possibilities[:gender_mfu])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Document_locations begin
        reference_locations ~ Reference_locations
        reference_calendar ~ Reference_calendar
    end

    @class Documents_to_be_destroyed begin
        employees ~ Employees
        reference_calendar ~ Reference_calendar
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        document_locations ~ Document_locations
        documents_to_be_destroyed ~ Documents_to_be_destroyed
    end
end

query = @query CreDocTrackingDBModel.Obs [
    reference_document_types_document_type_code document_locations.all_documents.reference_document_types.document_type_code
    reference_document_types_document_type_name document_locations.all_documents.reference_document_types.document_type_name
    reference_document_types_document_type_description document_locations.all_documents.reference_document_types.document_type_description
    reference_calendar_calendar_date document_locations.all_documents.reference_calendar.calendar_date
    reference_calendar_day_number document_locations.all_documents.reference_calendar.day_number
    reference_locations_location_code document_locations.reference_locations.location_code
    reference_locations_location_name document_locations.reference_locations.location_name
    reference_locations_location_description document_locations.reference_locations.location_description
    roles_role_code documents_to_be_destroyed.employees.roles.role_code
    roles_role_name documents_to_be_destroyed.employees.roles.role_name
    roles_role_description documents_to_be_destroyed.employees.roles.role_description
    all_documents_document_id document_locations.all_documents.document_id
    all_documents_document_name document_locations.all_documents.document_name
    all_documents_document_description document_locations.all_documents.document_description
    all_documents_other_details document_locations.all_documents.other_details
    employees_employee_id documents_to_be_destroyed.employees.employee_id
    employees_employee_name documents_to_be_destroyed.employees.employee_name
    employees_gender_mfu documents_to_be_destroyed.employees.gender_mfu
    employees_date_of_birth documents_to_be_destroyed.employees.date_of_birth
    employees_other_details documents_to_be_destroyed.employees.other_details
    documents_to_be_destroyed_other_details documents_to_be_destroyed.other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
