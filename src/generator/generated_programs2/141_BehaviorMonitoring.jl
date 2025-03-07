using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference_address_types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference_address_types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "address type code"], Any[0, "address type description"], Any[1, "detention type code"], Any[1, "detention type description"], Any[2, "incident type code"], Any[2, "incident type description"], Any[3, "address id"], Any[3, "line 1"], Any[3, "line 2"], Any[3, "line 3"], Any[3, "city"], Any[3, "zip postcode"], Any[3, "state province county"], Any[3, "country"], Any[3, "other address details"], Any[4, "student id"], Any[4, "address id"], Any[4, "first name"], Any[4, "middle name"], Any[4, "last name"], Any[4, "cell mobile number"], Any[4, "email address"], Any[4, "date first rental"], Any[4, "date left university"], Any[4, "other student details"], Any[5, "teacher id"], Any[5, "address id"], Any[5, "first name"], Any[5, "middle name"], Any[5, "last name"], Any[5, "gender"], Any[5, "cell mobile number"], Any[5, "email address"], Any[5, "other details"], Any[6, "notes id"], Any[6, "student id"], Any[6, "teacher id"], Any[6, "date of notes"], Any[6, "text of notes"], Any[6, "other details"], Any[7, "incident id"], Any[7, "incident type code"], Any[7, "student id"], Any[7, "date incident start"], Any[7, "date incident end"], Any[7, "incident summary"], Any[7, "recommendations"], Any[7, "other details"], Any[8, "detention id"], Any[8, "detention type code"], Any[8, "teacher id"], Any[8, "datetime detention start"], Any[8, "datetime detention end"], Any[8, "detention summary"], Any[8, "other details"], Any[9, "student id"], Any[9, "address id"], Any[9, "date address from"], Any[9, "date address to"], Any[9, "monthly rental"], Any[9, "other details"], Any[10, "student id"], Any[10, "detention id"], Any[10, "incident id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[17, 7], Any[27, 7], Any[37, 26], Any[36, 16], Any[43, 16], Any[42, 5], Any[51, 26], Any[50, 3], Any[56, 16], Any[57, 7], Any[62, 16], Any[63, 49], Any[64, 41]])
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







PClean.@model BehaviorMonitoringModel begin
    @class Reference_address_types begin
        address_type_code ~ ChooseUniformly(possibilities[:address_type_code])
        address_type_description ~ ChooseUniformly(possibilities[:address_type_description])
    end

    @class Reference_detention_type begin
        detention_type_code ~ ChooseUniformly(possibilities[:detention_type_code])
        detention_type_description ~ ChooseUniformly(possibilities[:detention_type_description])
    end

    @class Reference_incident_type begin
        incident_type_code ~ ChooseUniformly(possibilities[:incident_type_code])
        incident_type_description ~ ChooseUniformly(possibilities[:incident_type_description])
    end

    @class Addresses begin
        line_1 ~ ChooseUniformly(possibilities[:line_1])
        line_2 ~ ChooseUniformly(possibilities[:line_2])
        line_3 ~ ChooseUniformly(possibilities[:line_3])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
        other_address_details ~ ChooseUniformly(possibilities[:other_address_details])
    end

    @class Students begin
        addresses ~ Addresses
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        cell_mobile_number ~ ChooseUniformly(possibilities[:cell_mobile_number])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        date_first_rental ~ TimePrior(possibilities[:date_first_rental])
        date_left_university ~ TimePrior(possibilities[:date_left_university])
        other_student_details ~ ChooseUniformly(possibilities[:other_student_details])
    end

    @class Teachers begin
        addresses ~ Addresses
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        cell_mobile_number ~ ChooseUniformly(possibilities[:cell_mobile_number])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Assessment_notes begin
        notes_id ~ Unmodeled()
        students ~ Students
        teachers ~ Teachers
        date_of_notes ~ TimePrior(possibilities[:date_of_notes])
        text_of_notes ~ ChooseUniformly(possibilities[:text_of_notes])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Behavior_incident begin
        reference_incident_type ~ Reference_incident_type
        students ~ Students
        date_incident_start ~ TimePrior(possibilities[:date_incident_start])
        date_incident_end ~ TimePrior(possibilities[:date_incident_end])
        incident_summary ~ ChooseUniformly(possibilities[:incident_summary])
        recommendations ~ ChooseUniformly(possibilities[:recommendations])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Detention begin
        reference_detention_type ~ Reference_detention_type
        teachers ~ Teachers
        datetime_detention_start ~ TimePrior(possibilities[:datetime_detention_start])
        datetime_detention_end ~ TimePrior(possibilities[:datetime_detention_end])
        detention_summary ~ ChooseUniformly(possibilities[:detention_summary])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Student_addresses begin
        students ~ Students
        addresses ~ Addresses
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
        monthly_rental ~ ChooseUniformly(possibilities[:monthly_rental])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Students_in_detention begin
        students ~ Students
        detention ~ Detention
        behavior_incident ~ Behavior_incident
    end

    @class Obs begin
        reference_address_types ~ Reference_address_types
        assessment_notes ~ Assessment_notes
        student_addresses ~ Student_addresses
        students_in_detention ~ Students_in_detention
    end
end

query = @query BehaviorMonitoringModel.Obs [
    reference_address_types_address_type_code reference_address_types.address_type_code
    reference_address_types_address_type_description reference_address_types.address_type_description
    reference_detention_type_detention_type_code students_in_detention.detention.reference_detention_type.detention_type_code
    reference_detention_type_detention_type_description students_in_detention.detention.reference_detention_type.detention_type_description
    reference_incident_type_incident_type_code students_in_detention.behavior_incident.reference_incident_type.incident_type_code
    reference_incident_type_incident_type_description students_in_detention.behavior_incident.reference_incident_type.incident_type_description
    addresses_address_id student_addresses.students.addresses.address_id
    addresses_line_1 student_addresses.students.addresses.line_1
    addresses_line_2 student_addresses.students.addresses.line_2
    addresses_line_3 student_addresses.students.addresses.line_3
    addresses_city student_addresses.students.addresses.city
    addresses_zip_postcode student_addresses.students.addresses.zip_postcode
    addresses_state_province_county student_addresses.students.addresses.state_province_county
    addresses_country student_addresses.students.addresses.country
    addresses_other_address_details student_addresses.students.addresses.other_address_details
    students_student_id assessment_notes.students.student_id
    students_first_name assessment_notes.students.first_name
    students_middle_name assessment_notes.students.middle_name
    students_last_name assessment_notes.students.last_name
    students_cell_mobile_number assessment_notes.students.cell_mobile_number
    students_email_address assessment_notes.students.email_address
    students_date_first_rental assessment_notes.students.date_first_rental
    students_date_left_university assessment_notes.students.date_left_university
    students_other_student_details assessment_notes.students.other_student_details
    teachers_teacher_id assessment_notes.teachers.teacher_id
    teachers_first_name assessment_notes.teachers.first_name
    teachers_middle_name assessment_notes.teachers.middle_name
    teachers_last_name assessment_notes.teachers.last_name
    teachers_gender assessment_notes.teachers.gender
    teachers_cell_mobile_number assessment_notes.teachers.cell_mobile_number
    teachers_email_address assessment_notes.teachers.email_address
    teachers_other_details assessment_notes.teachers.other_details
    assessment_notes_notes_id assessment_notes.notes_id
    assessment_notes_date_of_notes assessment_notes.date_of_notes
    assessment_notes_text_of_notes assessment_notes.text_of_notes
    assessment_notes_other_details assessment_notes.other_details
    behavior_incident_incident_id students_in_detention.behavior_incident.incident_id
    behavior_incident_date_incident_start students_in_detention.behavior_incident.date_incident_start
    behavior_incident_date_incident_end students_in_detention.behavior_incident.date_incident_end
    behavior_incident_incident_summary students_in_detention.behavior_incident.incident_summary
    behavior_incident_recommendations students_in_detention.behavior_incident.recommendations
    behavior_incident_other_details students_in_detention.behavior_incident.other_details
    detention_id students_in_detention.detention.detention_id
    datetime_detention_start students_in_detention.detention.datetime_detention_start
    datetime_detention_end students_in_detention.detention.datetime_detention_end
    detention_summary students_in_detention.detention.detention_summary
    detention_other_details students_in_detention.detention.other_details
    student_addresses_date_address_from student_addresses.date_address_from
    student_addresses_date_address_to student_addresses.date_address_to
    student_addresses_monthly_rental student_addresses.monthly_rental
    student_addresses_other_details student_addresses.other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
