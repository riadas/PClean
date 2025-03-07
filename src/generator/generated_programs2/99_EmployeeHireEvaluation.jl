using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("employee_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("employee_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[1, "shop id"], Any[1, "name"], Any[1, "location"], Any[1, "district"], Any[1, "number products"], Any[1, "manager name"], Any[2, "shop id"], Any[2, "employee id"], Any[2, "start from"], Any[2, "is full time"], Any[3, "employee id"], Any[3, "year awarded"], Any[3, "bonus"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[1, "shop id"], Any[1, "name"], Any[1, "location"], Any[1, "district"], Any[1, "number products"], Any[1, "manager name"], Any[2, "shop id"], Any[2, "employee id"], Any[2, "start from"], Any[2, "is full time"], Any[3, "employee id"], Any[3, "year awarded"], Any[3, "bonus"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[1, "shop id"], Any[1, "name"], Any[1, "location"], Any[1, "district"], Any[1, "number products"], Any[1, "manager name"], Any[2, "shop id"], Any[2, "employee id"], Any[2, "start from"], Any[2, "is full time"], Any[3, "employee id"], Any[3, "year awarded"], Any[3, "bonus"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[1, "shop id"], Any[1, "name"], Any[1, "location"], Any[1, "district"], Any[1, "number products"], Any[1, "manager name"], Any[2, "shop id"], Any[2, "employee id"], Any[2, "start from"], Any[2, "is full time"], Any[3, "employee id"], Any[3, "year awarded"], Any[3, "bonus"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[1, "shop id"], Any[1, "name"], Any[1, "location"], Any[1, "district"], Any[1, "number products"], Any[1, "manager name"], Any[2, "shop id"], Any[2, "employee id"], Any[2, "start from"], Any[2, "is full time"], Any[3, "employee id"], Any[3, "year awarded"], Any[3, "bonus"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 1], Any[11, 5], Any[15, 1]])
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







PClean.@model EmployeeHireEvaluationModel begin
    @class Employee begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        city ~ ChooseUniformly(possibilities[:city])
    end

    @class Shop begin
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        district ~ ChooseUniformly(possibilities[:district])
        number_products ~ ChooseUniformly(possibilities[:number_products])
        manager_name ~ ChooseUniformly(possibilities[:manager_name])
    end

    @class Hiring begin
        shop ~ Shop
        start_from ~ ChooseUniformly(possibilities[:start_from])
        is_full_time ~ ChooseUniformly(possibilities[:is_full_time])
    end

    @class Evaluation begin
        employee ~ Employee
        year_awarded ~ ChooseUniformly(possibilities[:year_awarded])
        bonus ~ ChooseUniformly(possibilities[:bonus])
    end

    @class Obs begin
        hiring ~ Hiring
        evaluation ~ Evaluation
    end
end

query = @query EmployeeHireEvaluationModel.Obs [
    employee_id hiring.employee.employee_id
    employee_name hiring.employee.name
    employee_age hiring.employee.age
    employee_city hiring.employee.city
    shop_id hiring.shop.shop_id
    shop_name hiring.shop.name
    shop_location hiring.shop.location
    shop_district hiring.shop.district
    shop_number_products hiring.shop.number_products
    shop_manager_name hiring.shop.manager_name
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
