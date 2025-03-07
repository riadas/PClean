using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("regions_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("regions_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[1, "country id"], Any[1, "country name"], Any[1, "region id"], Any[2, "department id"], Any[2, "department name"], Any[2, "manager id"], Any[2, "location id"], Any[3, "job id"], Any[3, "job title"], Any[3, "min salary"], Any[3, "max salary"], Any[4, "employee id"], Any[4, "first name"], Any[4, "last name"], Any[4, "email"], Any[4, "phone number"], Any[4, "hire date"], Any[4, "job id"], Any[4, "salary"], Any[4, "commission pct"], Any[4, "manager id"], Any[4, "department id"], Any[5, "employee id"], Any[5, "start date"], Any[5, "end date"], Any[5, "job id"], Any[5, "department id"], Any[6, "location id"], Any[6, "street address"], Any[6, "postal code"], Any[6, "city"], Any[6, "state province"], Any[6, "country id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[1, "country id"], Any[1, "country name"], Any[1, "region id"], Any[2, "department id"], Any[2, "department name"], Any[2, "manager id"], Any[2, "location id"], Any[3, "job id"], Any[3, "job title"], Any[3, "min salary"], Any[3, "max salary"], Any[4, "employee id"], Any[4, "first name"], Any[4, "last name"], Any[4, "email"], Any[4, "phone number"], Any[4, "hire date"], Any[4, "job id"], Any[4, "salary"], Any[4, "commission pct"], Any[4, "manager id"], Any[4, "department id"], Any[5, "employee id"], Any[5, "start date"], Any[5, "end date"], Any[5, "job id"], Any[5, "department id"], Any[6, "location id"], Any[6, "street address"], Any[6, "postal code"], Any[6, "city"], Any[6, "state province"], Any[6, "country id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[1, "country id"], Any[1, "country name"], Any[1, "region id"], Any[2, "department id"], Any[2, "department name"], Any[2, "manager id"], Any[2, "location id"], Any[3, "job id"], Any[3, "job title"], Any[3, "min salary"], Any[3, "max salary"], Any[4, "employee id"], Any[4, "first name"], Any[4, "last name"], Any[4, "email"], Any[4, "phone number"], Any[4, "hire date"], Any[4, "job id"], Any[4, "salary"], Any[4, "commission pct"], Any[4, "manager id"], Any[4, "department id"], Any[5, "employee id"], Any[5, "start date"], Any[5, "end date"], Any[5, "job id"], Any[5, "department id"], Any[6, "location id"], Any[6, "street address"], Any[6, "postal code"], Any[6, "city"], Any[6, "state province"], Any[6, "country id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[1, "country id"], Any[1, "country name"], Any[1, "region id"], Any[2, "department id"], Any[2, "department name"], Any[2, "manager id"], Any[2, "location id"], Any[3, "job id"], Any[3, "job title"], Any[3, "min salary"], Any[3, "max salary"], Any[4, "employee id"], Any[4, "first name"], Any[4, "last name"], Any[4, "email"], Any[4, "phone number"], Any[4, "hire date"], Any[4, "job id"], Any[4, "salary"], Any[4, "commission pct"], Any[4, "manager id"], Any[4, "department id"], Any[5, "employee id"], Any[5, "start date"], Any[5, "end date"], Any[5, "job id"], Any[5, "department id"], Any[6, "location id"], Any[6, "street address"], Any[6, "postal code"], Any[6, "city"], Any[6, "state province"], Any[6, "country id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[1, "country id"], Any[1, "country name"], Any[1, "region id"], Any[2, "department id"], Any[2, "department name"], Any[2, "manager id"], Any[2, "location id"], Any[3, "job id"], Any[3, "job title"], Any[3, "min salary"], Any[3, "max salary"], Any[4, "employee id"], Any[4, "first name"], Any[4, "last name"], Any[4, "email"], Any[4, "phone number"], Any[4, "hire date"], Any[4, "job id"], Any[4, "salary"], Any[4, "commission pct"], Any[4, "manager id"], Any[4, "department id"], Any[5, "employee id"], Any[5, "start date"], Any[5, "end date"], Any[5, "job id"], Any[5, "department id"], Any[6, "location id"], Any[6, "street address"], Any[6, "postal code"], Any[6, "city"], Any[6, "state province"], Any[6, "country id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 1], Any[20, 10], Any[24, 6], Any[28, 10], Any[29, 6], Any[25, 14], Any[35, 3]])
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







PClean.@model Hr1Model begin
    @class Regions begin
        region_name ~ ChooseUniformly(possibilities[:region_name])
    end

    @class Countries begin
        country_id ~ ChooseUniformly(possibilities[:country_id])
        country_name ~ ChooseUniformly(possibilities[:country_name])
        regions ~ Regions
    end

    @class Departments begin
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
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        email ~ ChooseUniformly(possibilities[:email])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        hire_date ~ TimePrior(possibilities[:hire_date])
        jobs ~ Jobs
        salary ~ ChooseUniformly(possibilities[:salary])
        commission_pct ~ ChooseUniformly(possibilities[:commission_pct])
        manager_id ~ ChooseUniformly(possibilities[:manager_id])
        departments ~ Departments
    end

    @class Job_history begin
        start_date ~ TimePrior(possibilities[:start_date])
        end_date ~ TimePrior(possibilities[:end_date])
        jobs ~ Jobs
        departments ~ Departments
    end

    @class Locations begin
        street_address ~ ChooseUniformly(possibilities[:street_address])
        postal_code ~ ChooseUniformly(possibilities[:postal_code])
        city ~ ChooseUniformly(possibilities[:city])
        state_province ~ ChooseUniformly(possibilities[:state_province])
        countries ~ Countries
    end

    @class Obs begin
        job_history ~ Job_history
        locations ~ Locations
    end
end

query = @query Hr1Model.Obs [
    regions_region_id locations.countries.regions.region_id
    regions_region_name locations.countries.regions.region_name
    countries_country_id locations.countries.country_id
    countries_country_name locations.countries.country_name
    departments_department_id job_history.employees.departments.department_id
    departments_department_name job_history.employees.departments.department_name
    departments_manager_id job_history.employees.departments.manager_id
    departments_location_id job_history.employees.departments.location_id
    jobs_job_id job_history.employees.jobs.job_id
    jobs_job_title job_history.employees.jobs.job_title
    jobs_min_salary job_history.employees.jobs.min_salary
    jobs_max_salary job_history.employees.jobs.max_salary
    employees_employee_id job_history.employees.employee_id
    employees_first_name job_history.employees.first_name
    employees_last_name job_history.employees.last_name
    employees_email job_history.employees.email
    employees_phone_number job_history.employees.phone_number
    employees_hire_date job_history.employees.hire_date
    employees_salary job_history.employees.salary
    employees_commission_pct job_history.employees.commission_pct
    employees_manager_id job_history.employees.manager_id
    job_history_start_date job_history.start_date
    job_history_end_date job_history.end_date
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
