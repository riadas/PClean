using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1"], Any[0, "line 2"], Any[0, "line 3"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[0, "other address details"], Any[1, "course id"], Any[1, "course name"], Any[1, "course description"], Any[1, "other details"], Any[2, "department id"], Any[2, "department name"], Any[2, "department description"], Any[2, "other details"], Any[3, "degree program id"], Any[3, "department id"], Any[3, "degree summary name"], Any[3, "degree summary description"], Any[3, "other details"], Any[4, "section id"], Any[4, "course id"], Any[4, "section name"], Any[4, "section description"], Any[4, "other details"], Any[5, "semester id"], Any[5, "semester name"], Any[5, "semester description"], Any[5, "other details"], Any[6, "student id"], Any[6, "current address id"], Any[6, "permanent address id"], Any[6, "first name"], Any[6, "middle name"], Any[6, "last name"], Any[6, "cell mobile number"], Any[6, "email address"], Any[6, "ssn"], Any[6, "date first registered"], Any[6, "date left"], Any[6, "other student details"], Any[7, "student enrolment id"], Any[7, "degree program id"], Any[7, "semester id"], Any[7, "student id"], Any[7, "other details"], Any[8, "student course id"], Any[8, "course id"], Any[8, "student enrolment id"], Any[9, "transcript id"], Any[9, "transcript date"], Any[9, "other details"], Any[10, "student course id"], Any[10, "transcript id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[19, 14], Any[24, 10], Any[34, 1], Any[33, 1], Any[47, 32], Any[46, 28], Any[45, 18], Any[51, 44], Any[50, 10], Any[56, 52], Any[55, 49]])
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







PClean.@model StudentTranscriptsTrackingModel begin
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

    @class Courses begin
        course_name ~ ChooseUniformly(possibilities[:course_name])
        course_description ~ ChooseUniformly(possibilities[:course_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Departments begin
        department_name ~ ChooseUniformly(possibilities[:department_name])
        department_description ~ ChooseUniformly(possibilities[:department_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Degree_programs begin
        departments ~ Departments
        degree_summary_name ~ ChooseUniformly(possibilities[:degree_summary_name])
        degree_summary_description ~ ChooseUniformly(possibilities[:degree_summary_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Sections begin
        courses ~ Courses
        section_name ~ ChooseUniformly(possibilities[:section_name])
        section_description ~ ChooseUniformly(possibilities[:section_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Semesters begin
        semester_name ~ ChooseUniformly(possibilities[:semester_name])
        semester_description ~ ChooseUniformly(possibilities[:semester_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Students begin
        addresses ~ Addresses
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        cell_mobile_number ~ ChooseUniformly(possibilities[:cell_mobile_number])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        ssn ~ ChooseUniformly(possibilities[:ssn])
        date_first_registered ~ TimePrior(possibilities[:date_first_registered])
        date_left ~ TimePrior(possibilities[:date_left])
        other_student_details ~ ChooseUniformly(possibilities[:other_student_details])
    end

    @class Student_enrolment begin
        degree_programs ~ Degree_programs
        semesters ~ Semesters
        students ~ Students
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Student_enrolment_courses begin
        courses ~ Courses
        student_enrolment ~ Student_enrolment
    end

    @class Transcripts begin
        transcript_date ~ TimePrior(possibilities[:transcript_date])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Transcript_contents begin
        student_enrolment_courses ~ Student_enrolment_courses
        transcripts ~ Transcripts
    end

    @class Obs begin
        sections ~ Sections
        transcript_contents ~ Transcript_contents
    end
end

query = @query StudentTranscriptsTrackingModel.Obs [
    addresses_address_id transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.address_id
    addresses_line_1 transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.line_1
    addresses_line_2 transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.line_2
    addresses_line_3 transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.line_3
    addresses_city transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.city
    addresses_zip_postcode transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.zip_postcode
    addresses_state_province_county transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.state_province_county
    addresses_country transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.country
    addresses_other_address_details transcript_contents.student_enrolment_courses.student_enrolment.students.addresses.other_address_details
    courses_course_id sections.courses.course_id
    courses_course_name sections.courses.course_name
    courses_course_description sections.courses.course_description
    courses_other_details sections.courses.other_details
    departments_department_id transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.departments.department_id
    departments_department_name transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.departments.department_name
    departments_department_description transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.departments.department_description
    departments_other_details transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.departments.other_details
    degree_programs_degree_program_id transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.degree_program_id
    degree_programs_degree_summary_name transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.degree_summary_name
    degree_programs_degree_summary_description transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.degree_summary_description
    degree_programs_other_details transcript_contents.student_enrolment_courses.student_enrolment.degree_programs.other_details
    sections_section_id sections.section_id
    sections_section_name sections.section_name
    sections_section_description sections.section_description
    sections_other_details sections.other_details
    semesters_semester_id transcript_contents.student_enrolment_courses.student_enrolment.semesters.semester_id
    semesters_semester_name transcript_contents.student_enrolment_courses.student_enrolment.semesters.semester_name
    semesters_semester_description transcript_contents.student_enrolment_courses.student_enrolment.semesters.semester_description
    semesters_other_details transcript_contents.student_enrolment_courses.student_enrolment.semesters.other_details
    students_student_id transcript_contents.student_enrolment_courses.student_enrolment.students.student_id
    students_first_name transcript_contents.student_enrolment_courses.student_enrolment.students.first_name
    students_middle_name transcript_contents.student_enrolment_courses.student_enrolment.students.middle_name
    students_last_name transcript_contents.student_enrolment_courses.student_enrolment.students.last_name
    students_cell_mobile_number transcript_contents.student_enrolment_courses.student_enrolment.students.cell_mobile_number
    students_email_address transcript_contents.student_enrolment_courses.student_enrolment.students.email_address
    students_ssn transcript_contents.student_enrolment_courses.student_enrolment.students.ssn
    students_date_first_registered transcript_contents.student_enrolment_courses.student_enrolment.students.date_first_registered
    students_date_left transcript_contents.student_enrolment_courses.student_enrolment.students.date_left
    students_other_student_details transcript_contents.student_enrolment_courses.student_enrolment.students.other_student_details
    student_enrolment_id transcript_contents.student_enrolment_courses.student_enrolment.student_enrolment_id
    student_enrolment_other_details transcript_contents.student_enrolment_courses.student_enrolment.other_details
    student_enrolment_courses_student_course_id transcript_contents.student_enrolment_courses.student_course_id
    transcripts_transcript_id transcript_contents.transcripts.transcript_id
    transcripts_transcript_date transcript_contents.transcripts.transcript_date
    transcripts_other_details transcript_contents.transcripts.other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
