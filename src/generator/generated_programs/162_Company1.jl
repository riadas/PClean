using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("works on_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("works on_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "employee ssn"], Any[0, "project number"], Any[0, "hours"], Any[1, "first name"], Any[1, "minit"], Any[1, "last name"], Any[1, "ssn"], Any[1, "birth date"], Any[1, "address"], Any[1, "sex"], Any[1, "salary"], Any[1, "super ssn"], Any[1, "department no"], Any[2, "department name"], Any[2, "department number"], Any[2, "manager ssn"], Any[2, "manager start date"], Any[3, "dependent name"], Any[3, "dependent number"], Any[3, "dependent location"], Any[3, "department number"], Any[4, "employee ssn"], Any[4, "dependent name"], Any[4, "sex"], Any[4, "birth date"], Any[4, "relationship"], Any[5, "department number"], Any[5, "department location"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "employee ssn"], Any[0, "project number"], Any[0, "hours"], Any[1, "first name"], Any[1, "minit"], Any[1, "last name"], Any[1, "ssn"], Any[1, "birth date"], Any[1, "address"], Any[1, "sex"], Any[1, "salary"], Any[1, "super ssn"], Any[1, "department no"], Any[2, "department name"], Any[2, "department number"], Any[2, "manager ssn"], Any[2, "manager start date"], Any[3, "dependent name"], Any[3, "dependent number"], Any[3, "dependent location"], Any[3, "department number"], Any[4, "employee ssn"], Any[4, "dependent name"], Any[4, "sex"], Any[4, "birth date"], Any[4, "relationship"], Any[5, "department number"], Any[5, "department location"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = Any[]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "employee ssn"], Any[0, "project number"], Any[0, "hours"], Any[1, "first name"], Any[1, "minit"], Any[1, "last name"], Any[1, "ssn"], Any[1, "birth date"], Any[1, "address"], Any[1, "sex"], Any[1, "salary"], Any[1, "super ssn"], Any[1, "department no"], Any[2, "department name"], Any[2, "department number"], Any[2, "manager ssn"], Any[2, "manager start date"], Any[3, "dependent name"], Any[3, "dependent number"], Any[3, "dependent location"], Any[3, "department number"], Any[4, "employee ssn"], Any[4, "dependent name"], Any[4, "sex"], Any[4, "birth date"], Any[4, "relationship"], Any[5, "department number"], Any[5, "department location"]]
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





PClean.@model Company1Model begin
    @class Works_On begin
        employee_ssn ~ ChooseUniformly(possibilities[:employee_ssn])
        project_number ~ ChooseUniformly(possibilities[:project_number])
        hours ~ ChooseUniformly(possibilities[:hours])
    end

    @class Employee begin
        first_name ~ ChooseUniformly(possibilities[:first_name])
        minit ~ ChooseUniformly(possibilities[:minit])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        ssn ~ ChooseUniformly(possibilities[:ssn])
        birth_date ~ ChooseUniformly(possibilities[:birth_date])
        address ~ ChooseUniformly(possibilities[:address])
        sex ~ ChooseUniformly(possibilities[:sex])
        salary ~ ChooseUniformly(possibilities[:salary])
        super_ssn ~ ChooseUniformly(possibilities[:super_ssn])
        department_no ~ ChooseUniformly(possibilities[:department_no])
    end

    @class Department begin
        department_name ~ ChooseUniformly(possibilities[:department_name])
        department_number ~ ChooseUniformly(possibilities[:department_number])
        manager_ssn ~ ChooseUniformly(possibilities[:manager_ssn])
        manager_start_date ~ ChooseUniformly(possibilities[:manager_start_date])
    end

    @class Project begin
        dependent_name ~ ChooseUniformly(possibilities[:dependent_name])
        dependent_number ~ ChooseUniformly(possibilities[:dependent_number])
        dependent_location ~ ChooseUniformly(possibilities[:dependent_location])
        department_number ~ ChooseUniformly(possibilities[:department_number])
    end

    @class Dependent begin
        employee_ssn ~ ChooseUniformly(possibilities[:employee_ssn])
        dependent_name ~ ChooseUniformly(possibilities[:dependent_name])
        sex ~ ChooseUniformly(possibilities[:sex])
        birth_date ~ ChooseUniformly(possibilities[:birth_date])
        relationship ~ ChooseUniformly(possibilities[:relationship])
    end

    @class Department_Locations begin
        department_number ~ ChooseUniformly(possibilities[:department_number])
        department_location ~ ChooseUniformly(possibilities[:department_location])
    end

    @class Obs begin
        works_On ~ Works_On
        employee ~ Employee
        department ~ Department
        project ~ Project
        dependent ~ Dependent
        department_Locations ~ Department_Locations
    end
end

query = @query Company1Model.Obs [
    works_on_employee_ssn works_On.employee_ssn
    works_on_project_number works_On.project_number
    works_on_hours works_On.hours
    employee_first_name employee.first_name
    employee_minit employee.minit
    employee_last_name employee.last_name
    employee_ssn employee.ssn
    employee_birth_date employee.birth_date
    employee_address employee.address
    employee_sex employee.sex
    employee_salary employee.salary
    employee_super_ssn employee.super_ssn
    employee_department_no employee.department_no
    department_name department.department_name
    department_number department.department_number
    department_manager_ssn department.manager_ssn
    department_manager_start_date department.manager_start_date
    project_dependent_name project.dependent_name
    project_dependent_number project.dependent_number
    project_dependent_location project.dependent_location
    project_department_number project.department_number
    dependent_employee_ssn dependent.employee_ssn
    dependent_name dependent.dependent_name
    dependent_sex dependent.sex
    dependent_birth_date dependent.birth_date
    dependent_relationship dependent.relationship
    department_locations_department_number department_Locations.department_number
    department_locations_department_location department_Locations.department_location
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
