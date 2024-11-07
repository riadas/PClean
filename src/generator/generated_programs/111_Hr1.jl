using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("regions_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("regions_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[1, "country id"], Any[1, "country name"], Any[1, "region id"], Any[2, "department id"], Any[2, "department name"], Any[2, "manager id"], Any[2, "location id"], Any[3, "job id"], Any[3, "job title"], Any[3, "min salary"], Any[3, "max salary"], Any[4, "employee id"], Any[4, "first name"], Any[4, "last name"], Any[4, "email"], Any[4, "phone number"], Any[4, "hire date"], Any[4, "job id"], Any[4, "salary"], Any[4, "commission pct"], Any[4, "manager id"], Any[4, "department id"], Any[5, "employee id"], Any[5, "start date"], Any[5, "end date"], Any[5, "job id"], Any[5, "department id"], Any[6, "location id"], Any[6, "street address"], Any[6, "postal code"], Any[6, "city"], Any[6, "state province"], Any[6, "country id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[1, "country id"], Any[1, "country name"], Any[1, "region id"], Any[2, "department id"], Any[2, "department name"], Any[2, "manager id"], Any[2, "location id"], Any[3, "job id"], Any[3, "job title"], Any[3, "min salary"], Any[3, "max salary"], Any[4, "employee id"], Any[4, "first name"], Any[4, "last name"], Any[4, "email"], Any[4, "phone number"], Any[4, "hire date"], Any[4, "job id"], Any[4, "salary"], Any[4, "commission pct"], Any[4, "manager id"], Any[4, "department id"], Any[5, "employee id"], Any[5, "start date"], Any[5, "end date"], Any[5, "job id"], Any[5, "department id"], Any[6, "location id"], Any[6, "street address"], Any[6, "postal code"], Any[6, "city"], Any[6, "state province"], Any[6, "country id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Hr1Model begin
    @class Regions begin
        region_id ~ Unmodeled()
        region_name ~ ChooseUniformly(possibilities[:region_name])
    end

    @class Countries begin
        country_id ~ ChooseUniformly(possibilities[:country_id])
        country_name ~ ChooseUniformly(possibilities[:country_name])
        region_id ~ ChooseUniformly(possibilities[:region_id])
    end

    @class Departments begin
        department_id ~ Unmodeled()
        department_name ~ ChooseUniformly(possibilities[:department_name])
        manager_id ~ ChooseUniformly(possibilities[:manager_id])
        location_id ~ ChooseUniformly(possibilities[:location_id])
    end

    @class Jobs begin
        job_id ~ ChooseUniformly(possibilities[:job_id])
        job_title ~ ChooseUniformly(possibilities[:job_title])
        min_salary ~ ChooseUniformly(possibilities[:min_salary])
        max_salary ~ ChooseUniformly(possibilities[:max_salary])
    end

    @class Employees begin
        employee_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        email ~ ChooseUniformly(possibilities[:email])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        hire_date ~ TimePrior(possibilities[:hire_date])
        job_id ~ ChooseUniformly(possibilities[:job_id])
        salary ~ ChooseUniformly(possibilities[:salary])
        commission_pct ~ ChooseUniformly(possibilities[:commission_pct])
        manager_id ~ ChooseUniformly(possibilities[:manager_id])
        department_id ~ ChooseUniformly(possibilities[:department_id])
    end

    @class Job_History begin
        employee_id ~ Unmodeled()
        start_date ~ TimePrior(possibilities[:start_date])
        end_date ~ TimePrior(possibilities[:end_date])
        job_id ~ ChooseUniformly(possibilities[:job_id])
        department_id ~ ChooseUniformly(possibilities[:department_id])
    end

    @class Locations begin
        location_id ~ Unmodeled()
        street_address ~ ChooseUniformly(possibilities[:street_address])
        postal_code ~ ChooseUniformly(possibilities[:postal_code])
        city ~ ChooseUniformly(possibilities[:city])
        state_province ~ ChooseUniformly(possibilities[:state_province])
        country_id ~ ChooseUniformly(possibilities[:country_id])
    end

    @class Obs begin
        regions ~ Regions
        countries ~ Countries
        departments ~ Departments
        jobs ~ Jobs
        employees ~ Employees
        job_History ~ Job_History
        locations ~ Locations
    end
end

query = @query Hr1Model.Obs [
    regions_region_id regions.region_id
    regions_region_name regions.region_name
    countries_country_id countries.country_id
    countries_country_name countries.country_name
    departments_department_id departments.department_id
    departments_department_name departments.department_name
    departments_manager_id departments.manager_id
    departments_location_id departments.location_id
    jobs_job_id jobs.job_id
    jobs_job_title jobs.job_title
    jobs_min_salary jobs.min_salary
    jobs_max_salary jobs.max_salary
    employees_employee_id employees.employee_id
    employees_first_name employees.first_name
    employees_last_name employees.last_name
    employees_email employees.email
    employees_phone_number employees.phone_number
    employees_hire_date employees.hire_date
    employees_salary employees.salary
    employees_commission_pct employees.commission_pct
    employees_manager_id employees.manager_id
    job_history_start_date job_History.start_date
    job_history_end_date job_History.end_date
    locations_location_id locations.location_id
    locations_street_address locations.street_address
    locations_postal_code locations.postal_code
    locations_city locations.city
    locations_state_province locations.state_province
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
