using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference address types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference address types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address type code"], Any[0, "address type description"], Any[1, "detention type code"], Any[1, "detention type description"], Any[2, "incident type code"], Any[2, "incident type description"], Any[3, "address id"], Any[3, "line 1"], Any[3, "line 2"], Any[3, "line 3"], Any[3, "city"], Any[3, "zip postcode"], Any[3, "state province county"], Any[3, "country"], Any[3, "other address details"], Any[4, "student id"], Any[4, "address id"], Any[4, "first name"], Any[4, "middle name"], Any[4, "last name"], Any[4, "cell mobile number"], Any[4, "email address"], Any[4, "date first rental"], Any[4, "date left university"], Any[4, "other student details"], Any[5, "teacher id"], Any[5, "address id"], Any[5, "first name"], Any[5, "middle name"], Any[5, "last name"], Any[5, "gender"], Any[5, "cell mobile number"], Any[5, "email address"], Any[5, "other details"], Any[6, "notes id"], Any[6, "student id"], Any[6, "teacher id"], Any[6, "date of notes"], Any[6, "text of notes"], Any[6, "other details"], Any[7, "incident id"], Any[7, "incident type code"], Any[7, "student id"], Any[7, "date incident start"], Any[7, "date incident end"], Any[7, "incident summary"], Any[7, "recommendations"], Any[7, "other details"], Any[8, "detention id"], Any[8, "detention type code"], Any[8, "teacher id"], Any[8, "datetime detention start"], Any[8, "datetime detention end"], Any[8, "detention summary"], Any[8, "other details"], Any[9, "student id"], Any[9, "address id"], Any[9, "date address from"], Any[9, "date address to"], Any[9, "monthly rental"], Any[9, "other details"], Any[10, "student id"], Any[10, "detention id"], Any[10, "incident id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address type code"], Any[0, "address type description"], Any[1, "detention type code"], Any[1, "detention type description"], Any[2, "incident type code"], Any[2, "incident type description"], Any[3, "address id"], Any[3, "line 1"], Any[3, "line 2"], Any[3, "line 3"], Any[3, "city"], Any[3, "zip postcode"], Any[3, "state province county"], Any[3, "country"], Any[3, "other address details"], Any[4, "student id"], Any[4, "address id"], Any[4, "first name"], Any[4, "middle name"], Any[4, "last name"], Any[4, "cell mobile number"], Any[4, "email address"], Any[4, "date first rental"], Any[4, "date left university"], Any[4, "other student details"], Any[5, "teacher id"], Any[5, "address id"], Any[5, "first name"], Any[5, "middle name"], Any[5, "last name"], Any[5, "gender"], Any[5, "cell mobile number"], Any[5, "email address"], Any[5, "other details"], Any[6, "notes id"], Any[6, "student id"], Any[6, "teacher id"], Any[6, "date of notes"], Any[6, "text of notes"], Any[6, "other details"], Any[7, "incident id"], Any[7, "incident type code"], Any[7, "student id"], Any[7, "date incident start"], Any[7, "date incident end"], Any[7, "incident summary"], Any[7, "recommendations"], Any[7, "other details"], Any[8, "detention id"], Any[8, "detention type code"], Any[8, "teacher id"], Any[8, "datetime detention start"], Any[8, "datetime detention end"], Any[8, "detention summary"], Any[8, "other details"], Any[9, "student id"], Any[9, "address id"], Any[9, "date address from"], Any[9, "date address to"], Any[9, "monthly rental"], Any[9, "other details"], Any[10, "student id"], Any[10, "detention id"], Any[10, "incident id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["address id", "address id", "teacher id", "student id", "student id", "incident type code", "teacher id", "detention type code", "student id", "address id", "student id", "detention id", "incident id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "address type code"], Any[0, "address type description"], Any[1, "detention type description"], Any[2, "incident type description"], Any[3, "line 1"], Any[3, "line 2"], Any[3, "line 3"], Any[3, "city"], Any[3, "zip postcode"], Any[3, "state province county"], Any[3, "country"], Any[3, "other address details"], Any[4, "first name"], Any[4, "middle name"], Any[4, "last name"], Any[4, "cell mobile number"], Any[4, "email address"], Any[4, "date first rental"], Any[4, "date left university"], Any[4, "other student details"], Any[5, "first name"], Any[5, "middle name"], Any[5, "last name"], Any[5, "gender"], Any[5, "cell mobile number"], Any[5, "email address"], Any[5, "other details"], Any[6, "notes id"], Any[6, "date of notes"], Any[6, "text of notes"], Any[6, "other details"], Any[7, "date incident start"], Any[7, "date incident end"], Any[7, "incident summary"], Any[7, "recommendations"], Any[7, "other details"], Any[8, "datetime detention start"], Any[8, "datetime detention end"], Any[8, "detention summary"], Any[8, "other details"], Any[9, "date address from"], Any[9, "date address to"], Any[9, "monthly rental"], Any[9, "other details"]]
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





PClean.@model BehaviorMonitoringModel begin
    @class Reference_Address_Types begin
        address_type_code ~ ChooseUniformly(possibilities[:address_type_code])
        address_type_description ~ ChooseUniformly(possibilities[:address_type_description])
    end

    @class Reference_Detention_Type begin
        detention_type_code ~ ChooseUniformly(possibilities[:detention_type_code])
        detention_type_description ~ ChooseUniformly(possibilities[:detention_type_description])
    end

    @class Reference_Incident_Type begin
        incident_type_code ~ ChooseUniformly(possibilities[:incident_type_code])
        incident_type_description ~ ChooseUniformly(possibilities[:incident_type_description])
    end

    @class Addresses begin
        address_id ~ Unmodeled()
        line_1 ~ ChooseUniformly(possibilities[:line_1])
        line_2 ~ ChooseUniformly(possibilities[:line_2])
        line_3 ~ ChooseUniformly(possibilities[:line_3])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
        other_address_details ~ ChooseUniformly(possibilities[:other_address_details])
    end

    @class Obs begin
        reference_Address_Types ~ Reference_Address_Types
        reference_Detention_Type ~ Reference_Detention_Type
        reference_Incident_Type ~ Reference_Incident_Type
        addresses ~ Addresses
        student_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        cell_mobile_number ~ ChooseUniformly(possibilities[:cell_mobile_number])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        date_first_rental ~ TimePrior(possibilities[:date_first_rental])
        date_left_university ~ TimePrior(possibilities[:date_left_university])
        other_student_details ~ ChooseUniformly(possibilities[:other_student_details])
        teacher_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        cell_mobile_number ~ ChooseUniformly(possibilities[:cell_mobile_number])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        notes_id ~ Unmodeled()
        date_of_notes ~ TimePrior(possibilities[:date_of_notes])
        text_of_notes ~ ChooseUniformly(possibilities[:text_of_notes])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        incident_id ~ Unmodeled()
        date_incident_start ~ TimePrior(possibilities[:date_incident_start])
        date_incident_end ~ TimePrior(possibilities[:date_incident_end])
        incident_summary ~ ChooseUniformly(possibilities[:incident_summary])
        recommendations ~ ChooseUniformly(possibilities[:recommendations])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        detention_id ~ Unmodeled()
        datetime_detention_start ~ TimePrior(possibilities[:datetime_detention_start])
        datetime_detention_end ~ TimePrior(possibilities[:datetime_detention_end])
        detention_summary ~ ChooseUniformly(possibilities[:detention_summary])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
        monthly_rental ~ ChooseUniformly(possibilities[:monthly_rental])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end
end

query = @query BehaviorMonitoringModel.Obs [
    reference_address_types_address_type_code reference_Address_Types.address_type_code
    reference_address_types_address_type_description reference_Address_Types.address_type_description
    reference_detention_type_detention_type_code reference_Detention_Type.detention_type_code
    reference_detention_type_detention_type_description reference_Detention_Type.detention_type_description
    reference_incident_type_incident_type_code reference_Incident_Type.incident_type_code
    reference_incident_type_incident_type_description reference_Incident_Type.incident_type_description
    addresses_address_id addresses.address_id
    addresses_line_1 addresses.line_1
    addresses_line_2 addresses.line_2
    addresses_line_3 addresses.line_3
    addresses_city addresses.city
    addresses_zip_postcode addresses.zip_postcode
    addresses_state_province_county addresses.state_province_county
    addresses_country addresses.country
    addresses_other_address_details addresses.other_address_details
    students_student_id student_id
    students_first_name first_name
    students_middle_name middle_name
    students_last_name last_name
    students_cell_mobile_number cell_mobile_number
    students_email_address email_address
    students_date_first_rental date_first_rental
    students_date_left_university date_left_university
    students_other_student_details other_student_details
    teachers_teacher_id teacher_id
    teachers_first_name first_name
    teachers_middle_name middle_name
    teachers_last_name last_name
    teachers_gender gender
    teachers_cell_mobile_number cell_mobile_number
    teachers_email_address email_address
    teachers_other_details other_details
    assessment_notes_notes_id notes_id
    assessment_notes_date_of_notes date_of_notes
    assessment_notes_text_of_notes text_of_notes
    assessment_notes_other_details other_details
    behavior_incident_incident_id incident_id
    behavior_incident_date_incident_start date_incident_start
    behavior_incident_date_incident_end date_incident_end
    behavior_incident_incident_summary incident_summary
    behavior_incident_recommendations recommendations
    behavior_incident_other_details other_details
    detention_id detention_id
    datetime_detention_start datetime_detention_start
    datetime_detention_end datetime_detention_end
    detention_summary detention_summary
    detention_other_details other_details
    student_addresses_date_address_from date_address_from
    student_addresses_date_address_to date_address_to
    student_addresses_monthly_rental monthly_rental
    student_addresses_other_details other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
