using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("course authors and tutors_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("course authors and tutors_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "author id"], Any[0, "author tutor atb"], Any[0, "login name"], Any[0, "password"], Any[0, "personal name"], Any[0, "middle name"], Any[0, "family name"], Any[0, "gender mf"], Any[0, "address line 1"], Any[1, "student id"], Any[1, "date of registration"], Any[1, "date of latest logon"], Any[1, "login name"], Any[1, "password"], Any[1, "personal name"], Any[1, "middle name"], Any[1, "family name"], Any[2, "subject id"], Any[2, "subject name"], Any[3, "course id"], Any[3, "author id"], Any[3, "subject id"], Any[3, "course name"], Any[3, "course description"], Any[4, "registration id"], Any[4, "student id"], Any[4, "course id"], Any[4, "date of enrolment"], Any[4, "date of completion"], Any[5, "registration id"], Any[5, "date test taken"], Any[5, "test result"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "author id"], Any[0, "author tutor atb"], Any[0, "login name"], Any[0, "password"], Any[0, "personal name"], Any[0, "middle name"], Any[0, "family name"], Any[0, "gender mf"], Any[0, "address line 1"], Any[1, "student id"], Any[1, "date of registration"], Any[1, "date of latest logon"], Any[1, "login name"], Any[1, "password"], Any[1, "personal name"], Any[1, "middle name"], Any[1, "family name"], Any[2, "subject id"], Any[2, "subject name"], Any[3, "course id"], Any[3, "author id"], Any[3, "subject id"], Any[3, "course name"], Any[3, "course description"], Any[4, "registration id"], Any[4, "student id"], Any[4, "course id"], Any[4, "date of enrolment"], Any[4, "date of completion"], Any[5, "registration id"], Any[5, "date test taken"], Any[5, "test result"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ELearningModel begin
    @class Course_Authors_And_Tutors begin
        author_id ~ Unmodeled()
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
        student_id ~ Unmodeled()
        date_of_registration ~ TimePrior(possibilities[:date_of_registration])
        date_of_latest_logon ~ TimePrior(possibilities[:date_of_latest_logon])
        login_name ~ ChooseUniformly(possibilities[:login_name])
        password ~ ChooseUniformly(possibilities[:password])
        personal_name ~ ChooseUniformly(possibilities[:personal_name])
        middle_name ~ ChooseUniformly(possibilities[:middle_name])
        family_name ~ ChooseUniformly(possibilities[:family_name])
    end

    @class Subjects begin
        subject_id ~ Unmodeled()
        subject_name ~ ChooseUniformly(possibilities[:subject_name])
    end

    @class Courses begin
        course_id ~ Unmodeled()
        author_id ~ ChooseUniformly(possibilities[:author_id])
        subject_id ~ ChooseUniformly(possibilities[:subject_id])
        course_name ~ ChooseUniformly(possibilities[:course_name])
        course_description ~ ChooseUniformly(possibilities[:course_description])
    end

    @class Student_Course_Enrolment begin
        registration_id ~ Unmodeled()
        student_id ~ ChooseUniformly(possibilities[:student_id])
        course_id ~ ChooseUniformly(possibilities[:course_id])
        date_of_enrolment ~ TimePrior(possibilities[:date_of_enrolment])
        date_of_completion ~ TimePrior(possibilities[:date_of_completion])
    end

    @class Student_Tests_Taken begin
        registration_id ~ Unmodeled()
        date_test_taken ~ TimePrior(possibilities[:date_test_taken])
        test_result ~ ChooseUniformly(possibilities[:test_result])
    end

    @class Obs begin
        course_Authors_And_Tutors ~ Course_Authors_And_Tutors
        students ~ Students
        subjects ~ Subjects
        courses ~ Courses
        student_Course_Enrolment ~ Student_Course_Enrolment
        student_Tests_Taken ~ Student_Tests_Taken
    end
end

query = @query ELearningModel.Obs [
    course_authors_and_tutors_author_id course_Authors_And_Tutors.author_id
    course_authors_and_tutors_author_tutor_atb course_Authors_And_Tutors.author_tutor_atb
    course_authors_and_tutors_login_name course_Authors_And_Tutors.login_name
    course_authors_and_tutors_password course_Authors_And_Tutors.password
    course_authors_and_tutors_personal_name course_Authors_And_Tutors.personal_name
    course_authors_and_tutors_middle_name course_Authors_And_Tutors.middle_name
    course_authors_and_tutors_family_name course_Authors_And_Tutors.family_name
    course_authors_and_tutors_gender_mf course_Authors_And_Tutors.gender_mf
    course_authors_and_tutors_address_line_1 course_Authors_And_Tutors.address_line_1
    students_student_id students.student_id
    students_date_of_registration students.date_of_registration
    students_date_of_latest_logon students.date_of_latest_logon
    students_login_name students.login_name
    students_password students.password
    students_personal_name students.personal_name
    students_middle_name students.middle_name
    students_family_name students.family_name
    subjects_subject_id subjects.subject_id
    subjects_subject_name subjects.subject_name
    courses_course_id courses.course_id
    courses_course_name courses.course_name
    courses_course_description courses.course_description
    student_course_enrolment_registration_id student_Course_Enrolment.registration_id
    student_course_enrolment_date_of_enrolment student_Course_Enrolment.date_of_enrolment
    student_course_enrolment_date_of_completion student_Course_Enrolment.date_of_completion
    student_tests_taken_date_test_taken student_Tests_Taken.date_test_taken
    student_tests_taken_test_result student_Tests_Taken.test_result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
