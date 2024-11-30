using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1"], Any[0, "line 2"], Any[0, "line 3"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "course id"], Any[1, "course name"], Any[1, "course description"], Any[1, "other details"], Any[2, "department id"], Any[2, "department name"], Any[2, "department description"], Any[2, "other details"], Any[3, "degree program id"], Any[3, "department id"], Any[3, "degree summary name"], Any[3, "degree summary description"], Any[3, "other details"], Any[4, "section id"], Any[4, "course id"], Any[4, "section name"], Any[4, "section description"], Any[4, "other details"], Any[5, "semester id"], Any[5, "semester name"], Any[5, "semester description"], Any[5, "other details"], Any[6, "student id"], Any[6, "current address id"], Any[6, "permanent address id"], Any[6, "first name"], Any[6, "middle name"], Any[6, "last name"], Any[6, "cell mobile number"], Any[6, "email address"], Any[6, "ssn"], Any[6, "date first registered"], Any[6, "date left"], Any[6, "other student details"], Any[7, "student enrolment id"], Any[7, "degree program id"], Any[7, "semester id"], Any[7, "student id"], Any[7, "other details"], Any[8, "student course id"], Any[8, "course id"], Any[8, "student enrolment id"], Any[9, "transcript id"], Any[9, "transcript date"], Any[9, "other details"], Any[10, "student course id"], Any[10, "transcript id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1"], Any[0, "line 2"], Any[0, "line 3"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "course id"], Any[1, "course name"], Any[1, "course description"], Any[1, "other details"], Any[2, "department id"], Any[2, "department name"], Any[2, "department description"], Any[2, "other details"], Any[3, "degree program id"], Any[3, "department id"], Any[3, "degree summary name"], Any[3, "degree summary description"], Any[3, "other details"], Any[4, "section id"], Any[4, "course id"], Any[4, "section name"], Any[4, "section description"], Any[4, "other details"], Any[5, "semester id"], Any[5, "semester name"], Any[5, "semester description"], Any[5, "other details"], Any[6, "student id"], Any[6, "current address id"], Any[6, "permanent address id"], Any[6, "first name"], Any[6, "middle name"], Any[6, "last name"], Any[6, "cell mobile number"], Any[6, "email address"], Any[6, "ssn"], Any[6, "date first registered"], Any[6, "date left"], Any[6, "other student details"], Any[7, "student enrolment id"], Any[7, "degree program id"], Any[7, "semester id"], Any[7, "student id"], Any[7, "other details"], Any[8, "student course id"], Any[8, "course id"], Any[8, "student enrolment id"], Any[9, "transcript id"], Any[9, "transcript date"], Any[9, "other details"], Any[10, "student course id"], Any[10, "transcript id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["department id", "course id", "permanent address id", "current address id", "student id", "semester id", "degree program id", "student enrolment id", "course id", "transcript id", "student course id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1"], Any[0, "line 2"], Any[0, "line 3"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "course name"], Any[1, "course description"], Any[1, "other details"], Any[2, "department name"], Any[2, "department description"], Any[2, "other details"], Any[3, "degree summary name"], Any[3, "degree summary description"], Any[3, "other details"], Any[4, "section id"], Any[4, "section name"], Any[4, "section description"], Any[4, "other details"], Any[5, "semester name"], Any[5, "semester description"], Any[5, "other details"], Any[6, "first name"], Any[6, "middle name"], Any[6, "last name"], Any[6, "cell mobile number"], Any[6, "email address"], Any[6, "ssn"], Any[6, "date first registered"], Any[6, "date left"], Any[6, "other student details"], Any[7, "other details"], Any[9, "transcript date"], Any[9, "other details"]]
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





PClean.@model StudentTranscriptsTrackingModel begin
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

    @class Courses begin
        course_id ~ Unmodeled()
        course_name ~ ChooseUniformly(possibilities[:course_name])
        course_description ~ ChooseUniformly(possibilities[:course_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Departments begin
        department_id ~ Unmodeled()
        department_name ~ ChooseUniformly(possibilities[:department_name])
        department_description ~ ChooseUniformly(possibilities[:department_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Semesters begin
        semester_id ~ Unmodeled()
        semester_name ~ ChooseUniformly(possibilities[:semester_name])
        semester_description ~ ChooseUniformly(possibilities[:semester_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Transcripts begin
        transcript_id ~ Unmodeled()
        transcript_date ~ TimePrior(possibilities[:transcript_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        addresses ~ Addresses
        courses ~ Courses
        departments ~ Departments
        semesters ~ Semesters
        transcripts ~ Transcripts
        degree_program_id ~ Unmodeled()
        degree_summary_name ~ ChooseUniformly(possibilities[:degree_summary_name])
        degree_summary_description ~ ChooseUniformly(possibilities[:degree_summary_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        section_id ~ Unmodeled()
        section_name ~ ChooseUniformly(possibilities[:section_name])
        section_description ~ ChooseUniformly(possibilities[:section_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        student_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        cell_mobile_number ~ ChooseUniformly(possibilities[:cell_mobile_number])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        ssn ~ ChooseUniformly(possibilities[:ssn])
        date_first_registered ~ TimePrior(possibilities[:date_first_registered])
        date_left ~ TimePrior(possibilities[:date_left])
        other_student_details ~ ChooseUniformly(possibilities[:other_student_details])
        student_enrolment_id ~ Unmodeled()
        other_details ~ ChooseUniformly(possibilities[:other_details])
        student_course_id ~ Unmodeled()
    end
end

query = @query StudentTranscriptsTrackingModel.Obs [
    addresses_address_id addresses.address_id
    addresses_line_1 addresses.line_1
    addresses_line_2 addresses.line_2
    addresses_line_3 addresses.line_3
    addresses_city addresses.city
    addresses_zip_postcode addresses.zip_postcode
    addresses_state_province_county addresses.state_province_county
    addresses_country addresses.country
    addresses_other_address_details addresses.other_address_details
    courses_course_id courses.course_id
    courses_course_name courses.course_name
    courses_course_description courses.course_description
    courses_other_details courses.other_details
    departments_department_id departments.department_id
    departments_department_name departments.department_name
    departments_department_description departments.department_description
    departments_other_details departments.other_details
    degree_programs_degree_program_id degree_program_id
    degree_programs_degree_summary_name degree_summary_name
    degree_programs_degree_summary_description degree_summary_description
    degree_programs_other_details other_details
    sections_section_id section_id
    sections_section_name section_name
    sections_section_description section_description
    sections_other_details other_details
    semesters_semester_id semesters.semester_id
    semesters_semester_name semesters.semester_name
    semesters_semester_description semesters.semester_description
    semesters_other_details semesters.other_details
    students_student_id student_id
    students_first_name first_name
    students_middle_name middle_name
    students_last_name last_name
    students_cell_mobile_number cell_mobile_number
    students_email_address email_address
    students_ssn ssn
    students_date_first_registered date_first_registered
    students_date_left date_left
    students_other_student_details other_student_details
    student_enrolment_id student_enrolment_id
    student_enrolment_other_details other_details
    student_enrolment_courses_student_course_id student_course_id
    transcripts_transcript_id transcripts.transcript_id
    transcripts_transcript_date transcripts.transcript_date
    transcripts_other_details transcripts.other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
