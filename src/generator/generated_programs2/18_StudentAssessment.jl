using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1"], Any[0, "line 2"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "person id"], Any[1, "first name"], Any[1, "middle name"], Any[1, "last name"], Any[1, "cell mobile number"], Any[1, "email address"], Any[1, "login name"], Any[1, "password"], Any[2, "student id"], Any[2, "student details"], Any[3, "course id"], Any[3, "course name"], Any[3, "course description"], Any[3, "other details"], Any[4, "person address id"], Any[4, "person id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "student id"], Any[5, "course id"], Any[5, "registration date"], Any[6, "student id"], Any[6, "course id"], Any[6, "date of attendance"], Any[7, "candidate id"], Any[7, "candidate details"], Any[8, "candidate id"], Any[8, "qualification"], Any[8, "assessment date"], Any[8, "asessment outcome code"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1"], Any[0, "line 2"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "person id"], Any[1, "first name"], Any[1, "middle name"], Any[1, "last name"], Any[1, "cell mobile number"], Any[1, "email address"], Any[1, "login name"], Any[1, "password"], Any[2, "student id"], Any[2, "student details"], Any[3, "course id"], Any[3, "course name"], Any[3, "course description"], Any[3, "other details"], Any[4, "person address id"], Any[4, "person id"], Any[4, "address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "student id"], Any[5, "course id"], Any[5, "registration date"], Any[6, "student id"], Any[6, "course id"], Any[6, "date of attendance"], Any[7, "candidate id"], Any[7, "candidate details"], Any[8, "candidate id"], Any[8, "qualification"], Any[8, "assessment date"], Any[8, "asessment outcome code"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["student id", "address id", "person id", "course id", "student id", "student id", "course id", "candidate id", "candidate id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "line 1"], Any[0, "line 2"], Any[0, "city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "first name"], Any[1, "middle name"], Any[1, "last name"], Any[1, "cell mobile number"], Any[1, "email address"], Any[1, "login name"], Any[1, "password"], Any[2, "student details"], Any[3, "course name"], Any[3, "course description"], Any[3, "other details"], Any[4, "person address id"], Any[4, "date from"], Any[4, "date to"], Any[5, "registration date"], Any[6, "date of attendance"], Any[7, "candidate details"], Any[8, "qualification"], Any[8, "assessment date"], Any[8, "asessment outcome code"]]
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





PClean.@model StudentAssessmentModel begin
    @class Addresses begin
        address_id ~ Unmodeled()
        line_1 ~ ChooseUniformly(possibilities[:line_1])
        line_2 ~ ChooseUniformly(possibilities[:line_2])
        city ~ ChooseUniformly(possibilities[:city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class People begin
        person_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        cell_mobile_number ~ ChooseUniformly(possibilities[:cell_mobile_number])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        login_name ~ ChooseUniformly(possibilities[:login_name])
        password ~ ChooseUniformly(possibilities[:password])
    end

    @class Students begin
        people ~ People
        student_details ~ ChooseUniformly(possibilities[:student_details])
    end

    @class Courses begin
        course_id ~ ChooseUniformly(possibilities[:course_id])
        course_name ~ ChooseUniformly(possibilities[:course_name])
        course_description ~ ChooseUniformly(possibilities[:course_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class People_Addresses begin
        person_address_id ~ Unmodeled()
        people ~ People
        addresses ~ Addresses
        date_from ~ TimePrior(possibilities[:date_from])
        date_to ~ TimePrior(possibilities[:date_to])
    end

    @class Student_Course_Registrations begin
        students ~ Students
        courses ~ Courses
        registration_date ~ TimePrior(possibilities[:registration_date])
    end

    @class Student_Course_Attendance begin
        student_Course_Registrations ~ Student_Course_Registrations
        student_Course_Registrations ~ Student_Course_Registrations
        date_of_attendance ~ TimePrior(possibilities[:date_of_attendance])
    end

    @class Candidates begin
        people ~ People
        candidate_details ~ ChooseUniformly(possibilities[:candidate_details])
    end

    @class Candidate_Assessments begin
        candidates ~ Candidates
        qualification ~ ChooseUniformly(possibilities[:qualification])
        assessment_date ~ TimePrior(possibilities[:assessment_date])
        asessment_outcome_code ~ ChooseUniformly(possibilities[:asessment_outcome_code])
    end

    @class Obs begin
        people_Addresses ~ People_Addresses
        student_Course_Attendance ~ Student_Course_Attendance
        candidate_Assessments ~ Candidate_Assessments
    end
end

query = @query StudentAssessmentModel.Obs [
    addresses_address_id people_Addresses.addresses.address_id
    addresses_line_1 people_Addresses.addresses.line_1
    addresses_line_2 people_Addresses.addresses.line_2
    addresses_city people_Addresses.addresses.city
    addresses_zip_postcode people_Addresses.addresses.zip_postcode
    addresses_state_province_county people_Addresses.addresses.state_province_county
    addresses_country people_Addresses.addresses.country
    people_person_id people_Addresses.people.person_id
    people_first_name people_Addresses.people.first_name
    people_middle_name people_Addresses.people.middle_name
    people_last_name people_Addresses.people.last_name
    people_cell_mobile_number people_Addresses.people.cell_mobile_number
    people_email_address people_Addresses.people.email_address
    people_login_name people_Addresses.people.login_name
    people_password people_Addresses.people.password
    students_student_details student_Course_Attendance.student_Course_Registrations.students.student_details
    courses_course_id student_Course_Attendance.student_Course_Registrations.courses.course_id
    courses_course_name student_Course_Attendance.student_Course_Registrations.courses.course_name
    courses_course_description student_Course_Attendance.student_Course_Registrations.courses.course_description
    courses_other_details student_Course_Attendance.student_Course_Registrations.courses.other_details
    people_addresses_person_address_id people_Addresses.person_address_id
    people_addresses_date_from people_Addresses.date_from
    people_addresses_date_to people_Addresses.date_to
    student_course_registrations_registration_date student_Course_Attendance.student_Course_Registrations.registration_date
    student_course_attendance_date_of_attendance student_Course_Attendance.date_of_attendance
    candidates_candidate_details candidate_Assessments.candidates.candidate_details
    candidate_assessments_qualification candidate_Assessments.qualification
    candidate_assessments_assessment_date candidate_Assessments.assessment_date
    candidate_assessments_asessment_outcome_code candidate_Assessments.asessment_outcome_code
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
