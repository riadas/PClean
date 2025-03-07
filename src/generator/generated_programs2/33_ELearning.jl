using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("course_authors_and_tutors_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("course_authors_and_tutors_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "author id"], Any[0, "author tutor atb"], Any[0, "login name"], Any[0, "password"], Any[0, "personal name"], Any[0, "middle name"], Any[0, "family name"], Any[0, "gender mf"], Any[0, "address line 1"], Any[1, "student id"], Any[1, "date of registration"], Any[1, "date of latest logon"], Any[1, "login name"], Any[1, "password"], Any[1, "personal name"], Any[1, "middle name"], Any[1, "family name"], Any[2, "subject id"], Any[2, "subject name"], Any[3, "course id"], Any[3, "author id"], Any[3, "subject id"], Any[3, "course name"], Any[3, "course description"], Any[4, "registration id"], Any[4, "student id"], Any[4, "course id"], Any[4, "date of enrolment"], Any[4, "date of completion"], Any[5, "registration id"], Any[5, "date test taken"], Any[5, "test result"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "author id"], Any[0, "author tutor atb"], Any[0, "login name"], Any[0, "password"], Any[0, "personal name"], Any[0, "middle name"], Any[0, "family name"], Any[0, "gender mf"], Any[0, "address line 1"], Any[1, "student id"], Any[1, "date of registration"], Any[1, "date of latest logon"], Any[1, "login name"], Any[1, "password"], Any[1, "personal name"], Any[1, "middle name"], Any[1, "family name"], Any[2, "subject id"], Any[2, "subject name"], Any[3, "course id"], Any[3, "author id"], Any[3, "subject id"], Any[3, "course name"], Any[3, "course description"], Any[4, "registration id"], Any[4, "student id"], Any[4, "course id"], Any[4, "date of enrolment"], Any[4, "date of completion"], Any[5, "registration id"], Any[5, "date test taken"], Any[5, "test result"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "author id"], Any[0, "author tutor atb"], Any[0, "login name"], Any[0, "password"], Any[0, "personal name"], Any[0, "middle name"], Any[0, "family name"], Any[0, "gender mf"], Any[0, "address line 1"], Any[1, "student id"], Any[1, "date of registration"], Any[1, "date of latest logon"], Any[1, "login name"], Any[1, "password"], Any[1, "personal name"], Any[1, "middle name"], Any[1, "family name"], Any[2, "subject id"], Any[2, "subject name"], Any[3, "course id"], Any[3, "author id"], Any[3, "subject id"], Any[3, "course name"], Any[3, "course description"], Any[4, "registration id"], Any[4, "student id"], Any[4, "course id"], Any[4, "date of enrolment"], Any[4, "date of completion"], Any[5, "registration id"], Any[5, "date test taken"], Any[5, "test result"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "author id"], Any[0, "author tutor atb"], Any[0, "login name"], Any[0, "password"], Any[0, "personal name"], Any[0, "middle name"], Any[0, "family name"], Any[0, "gender mf"], Any[0, "address line 1"], Any[1, "student id"], Any[1, "date of registration"], Any[1, "date of latest logon"], Any[1, "login name"], Any[1, "password"], Any[1, "personal name"], Any[1, "middle name"], Any[1, "family name"], Any[2, "subject id"], Any[2, "subject name"], Any[3, "course id"], Any[3, "author id"], Any[3, "subject id"], Any[3, "course name"], Any[3, "course description"], Any[4, "registration id"], Any[4, "student id"], Any[4, "course id"], Any[4, "date of enrolment"], Any[4, "date of completion"], Any[5, "registration id"], Any[5, "date test taken"], Any[5, "test result"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "author id"], Any[0, "author tutor atb"], Any[0, "login name"], Any[0, "password"], Any[0, "personal name"], Any[0, "middle name"], Any[0, "family name"], Any[0, "gender mf"], Any[0, "address line 1"], Any[1, "student id"], Any[1, "date of registration"], Any[1, "date of latest logon"], Any[1, "login name"], Any[1, "password"], Any[1, "personal name"], Any[1, "middle name"], Any[1, "family name"], Any[2, "subject id"], Any[2, "subject name"], Any[3, "course id"], Any[3, "author id"], Any[3, "subject id"], Any[3, "course name"], Any[3, "course description"], Any[4, "registration id"], Any[4, "student id"], Any[4, "course id"], Any[4, "date of enrolment"], Any[4, "date of completion"], Any[5, "registration id"], Any[5, "date test taken"], Any[5, "test result"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[22, 18], Any[21, 1], Any[26, 10], Any[27, 20], Any[30, 25]])
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







PClean.@model ELearningModel begin
    @class Course_authors_and_tutors begin
        author_tutor_atb ~ ChooseUniformly(possibilities[:author_tutor_atb])
        login_name ~ ChooseUniformly(possibilities[:login_name])
        password ~ ChooseUniformly(possibilities[:password])
        personal_name ~ ChooseUniformly(possibilities[:personal_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        family_name ~ ChooseUniformly(possibilities[:family_name])
        gender_mf ~ ChooseUniformly(possibilities[:gender_mf])
        address_line_1 ~ ChooseUniformly(possibilities[:address_line_1])
    end

    @class Students begin
        date_of_registration ~ TimePrior(possibilities[:date_of_registration])
        date_of_latest_logon ~ TimePrior(possibilities[:date_of_latest_logon])
        login_name ~ ChooseUniformly(possibilities[:login_name])
        password ~ ChooseUniformly(possibilities[:password])
        personal_name ~ ChooseUniformly(possibilities[:personal_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        family_name ~ ChooseUniformly(possibilities[:family_name])
    end

    @class Subjects begin
        subject_name ~ ChooseUniformly(possibilities[:subject_name])
    end

    @class Courses begin
        course_authors_and_tutors ~ Course_authors_and_tutors
        subjects ~ Subjects
        course_name ~ ChooseUniformly(possibilities[:course_name])
        course_description ~ ChooseUniformly(possibilities[:course_description])
    end

    @class Student_course_enrolment begin
        students ~ Students
        courses ~ Courses
        date_of_enrolment ~ TimePrior(possibilities[:date_of_enrolment])
        date_of_completion ~ TimePrior(possibilities[:date_of_completion])
    end

    @class Student_tests_taken begin
        student_course_enrolment ~ Student_course_enrolment
        date_test_taken ~ TimePrior(possibilities[:date_test_taken])
        test_result ~ ChooseUniformly(possibilities[:test_result])
    end

    @class Obs begin
        student_tests_taken ~ Student_tests_taken
    end
end

query = @query ELearningModel.Obs [
    course_authors_and_tutors_author_id student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.author_id
    course_authors_and_tutors_author_tutor_atb student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.author_tutor_atb
    course_authors_and_tutors_login_name student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.login_name
    course_authors_and_tutors_password student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.password
    course_authors_and_tutors_personal_name student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.personal_name
    course_authors_and_tutors_middle_name student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.middle_name
    course_authors_and_tutors_family_name student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.family_name
    course_authors_and_tutors_gender_mf student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.gender_mf
    course_authors_and_tutors_address_line_1 student_tests_taken.student_course_enrolment.courses.course_authors_and_tutors.address_line_1
    students_student_id student_tests_taken.student_course_enrolment.students.student_id
    students_date_of_registration student_tests_taken.student_course_enrolment.students.date_of_registration
    students_date_of_latest_logon student_tests_taken.student_course_enrolment.students.date_of_latest_logon
    students_login_name student_tests_taken.student_course_enrolment.students.login_name
    students_password student_tests_taken.student_course_enrolment.students.password
    students_personal_name student_tests_taken.student_course_enrolment.students.personal_name
    students_middle_name student_tests_taken.student_course_enrolment.students.middle_name
    students_family_name student_tests_taken.student_course_enrolment.students.family_name
    subjects_subject_id student_tests_taken.student_course_enrolment.courses.subjects.subject_id
    subjects_subject_name student_tests_taken.student_course_enrolment.courses.subjects.subject_name
    courses_course_id student_tests_taken.student_course_enrolment.courses.course_id
    courses_course_name student_tests_taken.student_course_enrolment.courses.course_name
    courses_course_description student_tests_taken.student_course_enrolment.courses.course_description
    student_course_enrolment_registration_id student_tests_taken.student_course_enrolment.registration_id
    student_course_enrolment_date_of_enrolment student_tests_taken.student_course_enrolment.date_of_enrolment
    student_course_enrolment_date_of_completion student_tests_taken.student_course_enrolment.date_of_completion
    student_tests_taken_date_test_taken student_tests_taken.date_test_taken
    student_tests_taken_test_result student_tests_taken.test_result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
