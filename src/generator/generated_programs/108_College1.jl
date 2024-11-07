using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("class_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("class_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "class code"], Any[0, "course code"], Any[0, "class section"], Any[0, "class time"], Any[0, "class room"], Any[0, "professor employee number"], Any[1, "course code"], Any[1, "department code"], Any[1, "course description"], Any[1, "course credit"], Any[2, "department code"], Any[2, "department name"], Any[2, "school code"], Any[2, "employee number"], Any[2, "department address"], Any[2, "department extension"], Any[3, "employee number"], Any[3, "employee last name"], Any[3, "employee first name"], Any[3, "employee initial"], Any[3, "employee job code"], Any[3, "employee hire date"], Any[3, "employee date of birth"], Any[4, "class code"], Any[4, "student number"], Any[4, "enroll grade"], Any[5, "employee number"], Any[5, "department code"], Any[5, "professor office"], Any[5, "professor extension"], Any[5, "professor high degree"], Any[6, "student num"], Any[6, "student last name"], Any[6, "student first name"], Any[6, "student init"], Any[6, "student date of birth"], Any[6, "student class hours took"], Any[6, "student class"], Any[6, "student gpa"], Any[6, "student transfer"], Any[6, "department code"], Any[6, "student phone"], Any[6, "professor number"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "class code"], Any[0, "course code"], Any[0, "class section"], Any[0, "class time"], Any[0, "class room"], Any[0, "professor employee number"], Any[1, "course code"], Any[1, "department code"], Any[1, "course description"], Any[1, "course credit"], Any[2, "department code"], Any[2, "department name"], Any[2, "school code"], Any[2, "employee number"], Any[2, "department address"], Any[2, "department extension"], Any[3, "employee number"], Any[3, "employee last name"], Any[3, "employee first name"], Any[3, "employee initial"], Any[3, "employee job code"], Any[3, "employee hire date"], Any[3, "employee date of birth"], Any[4, "class code"], Any[4, "student number"], Any[4, "enroll grade"], Any[5, "employee number"], Any[5, "department code"], Any[5, "professor office"], Any[5, "professor extension"], Any[5, "professor high degree"], Any[6, "student num"], Any[6, "student last name"], Any[6, "student first name"], Any[6, "student init"], Any[6, "student date of birth"], Any[6, "student class hours took"], Any[6, "student class"], Any[6, "student gpa"], Any[6, "student transfer"], Any[6, "department code"], Any[6, "student phone"], Any[6, "professor number"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model College1Model begin
    @class Class begin
        class_code ~ ChooseUniformly(possibilities[:class_code])
        course_code ~ ChooseUniformly(possibilities[:course_code])
        class_section ~ ChooseUniformly(possibilities[:class_section])
        class_time ~ ChooseUniformly(possibilities[:class_time])
        class_room ~ ChooseUniformly(possibilities[:class_room])
        professor_employee_number ~ ChooseUniformly(possibilities[:professor_employee_number])
    end

    @class Course begin
        course_code ~ ChooseUniformly(possibilities[:course_code])
        department_code ~ ChooseUniformly(possibilities[:department_code])
        course_description ~ ChooseUniformly(possibilities[:course_description])
        course_credit ~ ChooseUniformly(possibilities[:course_credit])
    end

    @class Department begin
        department_code ~ ChooseUniformly(possibilities[:department_code])
        department_name ~ ChooseUniformly(possibilities[:department_name])
        school_code ~ ChooseUniformly(possibilities[:school_code])
        employee_number ~ ChooseUniformly(possibilities[:employee_number])
        department_address ~ ChooseUniformly(possibilities[:department_address])
        department_extension ~ ChooseUniformly(possibilities[:department_extension])
    end

    @class Employee begin
        employee_number ~ ChooseUniformly(possibilities[:employee_number])
        employee_last_name ~ ChooseUniformly(possibilities[:employee_last_name])
        employee_first_name ~ ChooseUniformly(possibilities[:employee_first_name])
        employee_initial ~ ChooseUniformly(possibilities[:employee_initial])
        employee_job_code ~ ChooseUniformly(possibilities[:employee_job_code])
        employee_hire_date ~ TimePrior(possibilities[:employee_hire_date])
        employee_date_of_birth ~ TimePrior(possibilities[:employee_date_of_birth])
    end

    @class Enroll begin
        class_code ~ ChooseUniformly(possibilities[:class_code])
        student_number ~ ChooseUniformly(possibilities[:student_number])
        enroll_grade ~ ChooseUniformly(possibilities[:enroll_grade])
    end

    @class Professor begin
        employee_number ~ ChooseUniformly(possibilities[:employee_number])
        department_code ~ ChooseUniformly(possibilities[:department_code])
        professor_office ~ ChooseUniformly(possibilities[:professor_office])
        professor_extension ~ ChooseUniformly(possibilities[:professor_extension])
        professor_high_degree ~ ChooseUniformly(possibilities[:professor_high_degree])
    end

    @class Student begin
        student_num ~ ChooseUniformly(possibilities[:student_num])
        student_last_name ~ ChooseUniformly(possibilities[:student_last_name])
        student_first_name ~ ChooseUniformly(possibilities[:student_first_name])
        student_init ~ ChooseUniformly(possibilities[:student_init])
        student_date_of_birth ~ TimePrior(possibilities[:student_date_of_birth])
        student_class_hours_took ~ ChooseUniformly(possibilities[:student_class_hours_took])
        student_class ~ ChooseUniformly(possibilities[:student_class])
        student_gpa ~ ChooseUniformly(possibilities[:student_gpa])
        student_transfer ~ ChooseUniformly(possibilities[:student_transfer])
        department_code ~ ChooseUniformly(possibilities[:department_code])
        student_phone ~ ChooseUniformly(possibilities[:student_phone])
        professor_number ~ ChooseUniformly(possibilities[:professor_number])
    end

    @class Obs begin
        class ~ Class
        course ~ Course
        department ~ Department
        employee ~ Employee
        enroll ~ Enroll
        professor ~ Professor
        student ~ Student
    end
end

query = @query College1Model.Obs [
    class_code class.class_code
    class_section class.class_section
    class_time class.class_time
    class_room class.class_room
    course_code course.course_code
    course_description course.course_description
    course_credit course.course_credit
    department_code department.department_code
    department_name department.department_name
    department_school_code department.school_code
    department_address department.department_address
    department_extension department.department_extension
    employee_number employee.employee_number
    employee_last_name employee.employee_last_name
    employee_first_name employee.employee_first_name
    employee_initial employee.employee_initial
    employee_job_code employee.employee_job_code
    employee_hire_date employee.employee_hire_date
    employee_date_of_birth employee.employee_date_of_birth
    enroll_grade enroll.enroll_grade
    professor_office professor.professor_office
    professor_extension professor.professor_extension
    professor_high_degree professor.professor_high_degree
    student_num student.student_num
    student_last_name student.student_last_name
    student_first_name student.student_first_name
    student_init student.student_init
    student_date_of_birth student.student_date_of_birth
    student_class_hours_took student.student_class_hours_took
    student_class student.student_class
    student_gpa student.student_gpa
    student_transfer student.student_transfer
    student_phone student.student_phone
    student_professor_number student.professor_number
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
