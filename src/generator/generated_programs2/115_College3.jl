using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("student_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("student_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "faculty id"], Any[1, "last name"], Any[1, "first name"], Any[1, "rank"], Any[1, "sex"], Any[1, "phone"], Any[1, "room"], Any[1, "building"], Any[2, "department number"], Any[2, "division"], Any[2, "department name"], Any[2, "room"], Any[2, "building"], Any[2, "department phone"], Any[3, "faculty id"], Any[3, "department number"], Any[3, "appt type"], Any[4, "course id"], Any[4, "course name"], Any[4, "credits"], Any[4, "instructor"], Any[4, "days"], Any[4, "hours"], Any[4, "department number"], Any[5, "student id"], Any[5, "department number"], Any[6, "student id"], Any[6, "course id"], Any[6, "grade"], Any[7, "letter grade"], Any[7, "grade point"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "faculty id"], Any[1, "last name"], Any[1, "first name"], Any[1, "rank"], Any[1, "sex"], Any[1, "phone"], Any[1, "room"], Any[1, "building"], Any[2, "department number"], Any[2, "division"], Any[2, "department name"], Any[2, "room"], Any[2, "building"], Any[2, "department phone"], Any[3, "faculty id"], Any[3, "department number"], Any[3, "appt type"], Any[4, "course id"], Any[4, "course name"], Any[4, "credits"], Any[4, "instructor"], Any[4, "days"], Any[4, "hours"], Any[4, "department number"], Any[5, "student id"], Any[5, "department number"], Any[6, "student id"], Any[6, "course id"], Any[6, "grade"], Any[7, "letter grade"], Any[7, "grade point"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "faculty id"], Any[1, "last name"], Any[1, "first name"], Any[1, "rank"], Any[1, "sex"], Any[1, "phone"], Any[1, "room"], Any[1, "building"], Any[2, "department number"], Any[2, "division"], Any[2, "department name"], Any[2, "room"], Any[2, "building"], Any[2, "department phone"], Any[3, "faculty id"], Any[3, "department number"], Any[3, "appt type"], Any[4, "course id"], Any[4, "course name"], Any[4, "credits"], Any[4, "instructor"], Any[4, "days"], Any[4, "hours"], Any[4, "department number"], Any[5, "student id"], Any[5, "department number"], Any[6, "student id"], Any[6, "course id"], Any[6, "grade"], Any[7, "letter grade"], Any[7, "grade point"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "faculty id"], Any[1, "last name"], Any[1, "first name"], Any[1, "rank"], Any[1, "sex"], Any[1, "phone"], Any[1, "room"], Any[1, "building"], Any[2, "department number"], Any[2, "division"], Any[2, "department name"], Any[2, "room"], Any[2, "building"], Any[2, "department phone"], Any[3, "faculty id"], Any[3, "department number"], Any[3, "appt type"], Any[4, "course id"], Any[4, "course name"], Any[4, "credits"], Any[4, "instructor"], Any[4, "days"], Any[4, "hours"], Any[4, "department number"], Any[5, "student id"], Any[5, "department number"], Any[6, "student id"], Any[6, "course id"], Any[6, "grade"], Any[7, "letter grade"], Any[7, "grade point"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "faculty id"], Any[1, "last name"], Any[1, "first name"], Any[1, "rank"], Any[1, "sex"], Any[1, "phone"], Any[1, "room"], Any[1, "building"], Any[2, "department number"], Any[2, "division"], Any[2, "department name"], Any[2, "room"], Any[2, "building"], Any[2, "department phone"], Any[3, "faculty id"], Any[3, "department number"], Any[3, "appt type"], Any[4, "course id"], Any[4, "course name"], Any[4, "credits"], Any[4, "instructor"], Any[4, "days"], Any[4, "hours"], Any[4, "department number"], Any[5, "student id"], Any[5, "department number"], Any[6, "student id"], Any[6, "course id"], Any[6, "grade"], Any[7, "letter grade"], Any[7, "grade point"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[24, 17], Any[23, 9], Any[32, 17], Any[29, 9], Any[34, 17], Any[33, 1], Any[37, 38], Any[36, 26], Any[35, 1]])
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







PClean.@model College3Model begin
    @class Student begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Faculty begin
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

    @class Member_of begin
        faculty ~ Faculty
        department ~ Department
        appt_type ~ ChooseUniformly(possibilities[:appt_type])
    end

    @class Course begin
        course_id ~ ChooseUniformly(possibilities[:course_id])
        course_name ~ ChooseUniformly(possibilities[:course_name])
        credits ~ ChooseUniformly(possibilities[:credits])
        faculty ~ Faculty
        days ~ ChooseUniformly(possibilities[:days])
        hours ~ ChooseUniformly(possibilities[:hours])
        department ~ Department
    end

    @class Minor_in begin
        student ~ Student
        department ~ Department
    end

    @class Grade_conversion begin
        letter_grade ~ ChooseUniformly(possibilities[:letter_grade])
        grade_point ~ ChooseUniformly(possibilities[:grade_point])
    end

    @class Enrolled_in begin
        student ~ Student
        course ~ Course
        grade_conversion ~ Grade_conversion
    end

    @class Obs begin
        member_of ~ Member_of
        minor_in ~ Minor_in
        enrolled_in ~ Enrolled_in
    end
end

query = @query College3Model.Obs [
    student_id minor_in.student.student_id
    student_last_name minor_in.student.last_name
    student_first_name minor_in.student.first_name
    student_age minor_in.student.age
    student_sex minor_in.student.sex
    student_major minor_in.student.major
    student_advisor minor_in.student.advisor
    student_city_code minor_in.student.city_code
    faculty_id member_of.faculty.faculty_id
    faculty_last_name member_of.faculty.last_name
    faculty_first_name member_of.faculty.first_name
    faculty_rank member_of.faculty.rank
    faculty_sex member_of.faculty.sex
    faculty_phone member_of.faculty.phone
    faculty_room member_of.faculty.room
    faculty_building member_of.faculty.building
    department_number member_of.department.department_number
    department_division member_of.department.division
    department_name member_of.department.department_name
    department_room member_of.department.room
    department_building member_of.department.building
    department_phone member_of.department.department_phone
    member_of_appt_type member_of.appt_type
    course_id enrolled_in.course.course_id
    course_name enrolled_in.course.course_name
    course_credits enrolled_in.course.credits
    course_days enrolled_in.course.days
    course_hours enrolled_in.course.hours
    grade_conversion_letter_grade enrolled_in.grade_conversion.letter_grade
    grade_conversion_grade_point enrolled_in.grade_conversion.grade_point
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
