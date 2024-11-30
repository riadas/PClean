using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("class_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("class_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "class code"], Any[0, "course code"], Any[0, "class section"], Any[0, "class time"], Any[0, "class room"], Any[0, "professor employee number"], Any[1, "course code"], Any[1, "department code"], Any[1, "course description"], Any[1, "course credit"], Any[2, "department code"], Any[2, "department name"], Any[2, "school code"], Any[2, "employee number"], Any[2, "department address"], Any[2, "department extension"], Any[3, "employee number"], Any[3, "employee last name"], Any[3, "employee first name"], Any[3, "employee initial"], Any[3, "employee job code"], Any[3, "employee hire date"], Any[3, "employee date of birth"], Any[4, "class code"], Any[4, "student number"], Any[4, "enroll grade"], Any[5, "employee number"], Any[5, "department code"], Any[5, "professor office"], Any[5, "professor extension"], Any[5, "professor high degree"], Any[6, "student num"], Any[6, "student last name"], Any[6, "student first name"], Any[6, "student init"], Any[6, "student date of birth"], Any[6, "student class hours took"], Any[6, "student class"], Any[6, "student gpa"], Any[6, "student transfer"], Any[6, "department code"], Any[6, "student phone"], Any[6, "professor number"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "class code"], Any[0, "course code"], Any[0, "class section"], Any[0, "class time"], Any[0, "class room"], Any[0, "professor employee number"], Any[1, "course code"], Any[1, "department code"], Any[1, "course description"], Any[1, "course credit"], Any[2, "department code"], Any[2, "department name"], Any[2, "school code"], Any[2, "employee number"], Any[2, "department address"], Any[2, "department extension"], Any[3, "employee number"], Any[3, "employee last name"], Any[3, "employee first name"], Any[3, "employee initial"], Any[3, "employee job code"], Any[3, "employee hire date"], Any[3, "employee date of birth"], Any[4, "class code"], Any[4, "student number"], Any[4, "enroll grade"], Any[5, "employee number"], Any[5, "department code"], Any[5, "professor office"], Any[5, "professor extension"], Any[5, "professor high degree"], Any[6, "student num"], Any[6, "student last name"], Any[6, "student first name"], Any[6, "student init"], Any[6, "student date of birth"], Any[6, "student class hours took"], Any[6, "student class"], Any[6, "student gpa"], Any[6, "student transfer"], Any[6, "department code"], Any[6, "student phone"], Any[6, "professor number"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["professor employee number", "course code", "department code", "employee number", "student number", "class code", "department code", "employee number", "department code"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "class section"], Any[0, "class time"], Any[0, "class room"], Any[1, "course description"], Any[1, "course credit"], Any[2, "department name"], Any[2, "school code"], Any[2, "department address"], Any[2, "department extension"], Any[3, "employee last name"], Any[3, "employee first name"], Any[3, "employee initial"], Any[3, "employee job code"], Any[3, "employee hire date"], Any[3, "employee date of birth"], Any[4, "enroll grade"], Any[5, "professor office"], Any[5, "professor extension"], Any[5, "professor high degree"], Any[6, "student num"], Any[6, "student last name"], Any[6, "student first name"], Any[6, "student init"], Any[6, "student date of birth"], Any[6, "student class hours took"], Any[6, "student class"], Any[6, "student gpa"], Any[6, "student transfer"], Any[6, "student phone"], Any[6, "professor number"]]
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





PClean.@model College1Model begin
    @class Employee begin
        employee_number ~ ChooseUniformly(possibilities[:employee_number])
        employee_last_name ~ ChooseUniformly(possibilities[:employee_last_name])
        employee_first_name ~ ChooseUniformly(possibilities[:employee_first_name])
        employee_initial ~ ChooseUniformly(possibilities[:employee_initial])
        employee_job_code ~ ChooseUniformly(possibilities[:employee_job_code])
        employee_hire_date ~ TimePrior(possibilities[:employee_hire_date])
        employee_date_of_birth ~ TimePrior(possibilities[:employee_date_of_birth])
    end

    @class Obs begin
        employee ~ Employee
        class_code ~ ChooseUniformly(possibilities[:class_code])
        class_section ~ ChooseUniformly(possibilities[:class_section])
        class_time ~ ChooseUniformly(possibilities[:class_time])
        class_room ~ ChooseUniformly(possibilities[:class_room])
        course_code ~ ChooseUniformly(possibilities[:course_code])
        course_description ~ ChooseUniformly(possibilities[:course_description])
        course_credit ~ ChooseUniformly(possibilities[:course_credit])
        department_code ~ ChooseUniformly(possibilities[:department_code])
        department_name ~ ChooseUniformly(possibilities[:department_name])
        school_code ~ ChooseUniformly(possibilities[:school_code])
        department_address ~ ChooseUniformly(possibilities[:department_address])
        department_extension ~ ChooseUniformly(possibilities[:department_extension])
        enroll_grade ~ ChooseUniformly(possibilities[:enroll_grade])
        professor_office ~ ChooseUniformly(possibilities[:professor_office])
        professor_extension ~ ChooseUniformly(possibilities[:professor_extension])
        professor_high_degree ~ ChooseUniformly(possibilities[:professor_high_degree])
        student_num ~ ChooseUniformly(possibilities[:student_num])
        student_last_name ~ ChooseUniformly(possibilities[:student_last_name])
        student_first_name ~ ChooseUniformly(possibilities[:student_first_name])
        student_init ~ ChooseUniformly(possibilities[:student_init])
        student_date_of_birth ~ TimePrior(possibilities[:student_date_of_birth])
        student_class_hours_took ~ ChooseUniformly(possibilities[:student_class_hours_took])
        student_class ~ ChooseUniformly(possibilities[:student_class])
        student_gpa ~ ChooseUniformly(possibilities[:student_gpa])
        student_transfer ~ ChooseUniformly(possibilities[:student_transfer])
        student_phone ~ ChooseUniformly(possibilities[:student_phone])
        professor_number ~ ChooseUniformly(possibilities[:professor_number])
    end
end

query = @query College1Model.Obs [
    class_code class_code
    class_section class_section
    class_time class_time
    class_room class_room
    course_code course_code
    course_description course_description
    course_credit course_credit
    department_code department_code
    department_name department_name
    department_school_code school_code
    department_address department_address
    department_extension department_extension
    employee_number employee.employee_number
    employee_last_name employee.employee_last_name
    employee_first_name employee.employee_first_name
    employee_initial employee.employee_initial
    employee_job_code employee.employee_job_code
    employee_hire_date employee.employee_hire_date
    employee_date_of_birth employee.employee_date_of_birth
    enroll_grade enroll_grade
    professor_office professor_office
    professor_extension professor_extension
    professor_high_degree professor_high_degree
    student_num student_num
    student_last_name student_last_name
    student_first_name student_first_name
    student_init student_init
    student_date_of_birth student_date_of_birth
    student_class_hours_took student_class_hours_took
    student_class student_class
    student_gpa student_gpa
    student_transfer student_transfer
    student_phone student_phone
    student_professor_number professor_number
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
