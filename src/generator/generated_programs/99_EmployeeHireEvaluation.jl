using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("employee_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("employee_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[1, "shop id"], Any[1, "name"], Any[1, "location"], Any[1, "district"], Any[1, "number products"], Any[1, "manager name"], Any[2, "shop id"], Any[2, "employee id"], Any[2, "start from"], Any[2, "is full time"], Any[3, "employee id"], Any[3, "year awarded"], Any[3, "bonus"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[1, "shop id"], Any[1, "name"], Any[1, "location"], Any[1, "district"], Any[1, "number products"], Any[1, "manager name"], Any[2, "shop id"], Any[2, "employee id"], Any[2, "start from"], Any[2, "is full time"], Any[3, "employee id"], Any[3, "year awarded"], Any[3, "bonus"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model EmployeeHireEvaluationModel begin
    @class Employee begin
        employee_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        city ~ ChooseUniformly(possibilities[:city])
    end

    @class Shop begin
        shop_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        district ~ ChooseUniformly(possibilities[:district])
        number_products ~ ChooseUniformly(possibilities[:number_products])
        manager_name ~ ChooseUniformly(possibilities[:manager_name])
    end

    @class Hiring begin
        shop_id ~ Unmodeled()
        employee_id ~ ChooseUniformly(possibilities[:employee_id])
        start_from ~ ChooseUniformly(possibilities[:start_from])
        is_full_time ~ ChooseUniformly(possibilities[:is_full_time])
    end

    @class Evaluation begin
        employee_id ~ ChooseUniformly(possibilities[:employee_id])
        year_awarded ~ ChooseUniformly(possibilities[:year_awarded])
        bonus ~ ChooseUniformly(possibilities[:bonus])
    end

    @class Obs begin
        employee ~ Employee
        shop ~ Shop
        hiring ~ Hiring
        evaluation ~ Evaluation
    end
end

query = @query EmployeeHireEvaluationModel.Obs [
    employee_id employee.employee_id
    employee_name employee.name
    employee_age employee.age
    employee_city employee.city
    shop_id shop.shop_id
    shop_name shop.name
    shop_location shop.location
    shop_district shop.district
    shop_number_products shop.number_products
    shop_manager_name shop.manager_name
    hiring_start_from hiring.start_from
    hiring_is_full_time hiring.is_full_time
    evaluation_year_awarded evaluation.year_awarded
    evaluation_bonus evaluation.bonus
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
