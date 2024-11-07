using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("student_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("student_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "faculty id"], Any[1, "last name"], Any[1, "first name"], Any[1, "rank"], Any[1, "sex"], Any[1, "phone"], Any[1, "room"], Any[1, "building"], Any[2, "department number"], Any[2, "division"], Any[2, "department name"], Any[2, "room"], Any[2, "building"], Any[2, "department phone"], Any[3, "faculty id"], Any[3, "department number"], Any[3, "appt type"], Any[4, "course id"], Any[4, "course name"], Any[4, "credits"], Any[4, "instructor"], Any[4, "days"], Any[4, "hours"], Any[4, "department number"], Any[5, "student id"], Any[5, "department number"], Any[6, "student id"], Any[6, "course id"], Any[6, "grade"], Any[7, "letter grade"], Any[7, "grade point"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "faculty id"], Any[1, "last name"], Any[1, "first name"], Any[1, "rank"], Any[1, "sex"], Any[1, "phone"], Any[1, "room"], Any[1, "building"], Any[2, "department number"], Any[2, "division"], Any[2, "department name"], Any[2, "room"], Any[2, "building"], Any[2, "department phone"], Any[3, "faculty id"], Any[3, "department number"], Any[3, "appt type"], Any[4, "course id"], Any[4, "course name"], Any[4, "credits"], Any[4, "instructor"], Any[4, "days"], Any[4, "hours"], Any[4, "department number"], Any[5, "student id"], Any[5, "department number"], Any[6, "student id"], Any[6, "course id"], Any[6, "grade"], Any[7, "letter grade"], Any[7, "grade point"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model College3Model begin
    @class Student begin
        student_id ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Faculty begin
        faculty_id ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        rank ~ ChooseUniformly(possibilities[:rank])
        sex ~ ChooseUniformly(possibilities[:sex])
        phone ~ ChooseUniformly(possibilities[:phone])
        room ~ ChooseUniformly(possibilities[:room])
        building ~ ChooseUniformly(possibilities[:building])
    end

    @class Department begin
        department_number ~ ChooseUniformly(possibilities[:department_number])
        division ~ ChooseUniformly(possibilities[:division])
        department_name ~ ChooseUniformly(possibilities[:department_name])
        room ~ ChooseUniformly(possibilities[:room])
        building ~ ChooseUniformly(possibilities[:building])
        department_phone ~ ChooseUniformly(possibilities[:department_phone])
    end

    @class Member_Of begin
        faculty_id ~ Unmodeled()
        department_number ~ ChooseUniformly(possibilities[:department_number])
        appt_type ~ ChooseUniformly(possibilities[:appt_type])
    end

    @class Course begin
        course_id ~ ChooseUniformly(possibilities[:course_id])
        course_name ~ ChooseUniformly(possibilities[:course_name])
        credits ~ ChooseUniformly(possibilities[:credits])
        instructor ~ ChooseUniformly(possibilities[:instructor])
        days ~ ChooseUniformly(possibilities[:days])
        hours ~ ChooseUniformly(possibilities[:hours])
        department_number ~ ChooseUniformly(possibilities[:department_number])
    end

    @class Minor_In begin
        student_id ~ Unmodeled()
        department_number ~ ChooseUniformly(possibilities[:department_number])
    end

    @class Enrolled_In begin
        student_id ~ Unmodeled()
        course_id ~ ChooseUniformly(possibilities[:course_id])
        grade ~ ChooseUniformly(possibilities[:grade])
    end

    @class Grade_Conversion begin
        letter_grade ~ ChooseUniformly(possibilities[:letter_grade])
        grade_point ~ ChooseUniformly(possibilities[:grade_point])
    end

    @class Obs begin
        student ~ Student
        faculty ~ Faculty
        department ~ Department
        member_Of ~ Member_Of
        course ~ Course
        minor_In ~ Minor_In
        enrolled_In ~ Enrolled_In
        grade_Conversion ~ Grade_Conversion
    end
end

query = @query College3Model.Obs [
    student_id student.student_id
    student_last_name student.last_name
    student_first_name student.first_name
    student_age student.age
    student_sex student.sex
    student_major student.major
    student_advisor student.advisor
    student_city_code student.city_code
    faculty_id faculty.faculty_id
    faculty_last_name faculty.last_name
    faculty_first_name faculty.first_name
    faculty_rank faculty.rank
    faculty_sex faculty.sex
    faculty_phone faculty.phone
    faculty_room faculty.room
    faculty_building faculty.building
    department_number department.department_number
    department_division department.division
    department_name department.department_name
    department_room department.room
    department_building department.building
    department_phone department.department_phone
    member_of_appt_type member_Of.appt_type
    course_id course.course_id
    course_name course.course_name
    course_credits course.credits
    course_days course.days
    course_hours course.hours
    grade_conversion_letter_grade grade_Conversion.letter_grade
    grade_conversion_grade_point grade_Conversion.grade_point
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
