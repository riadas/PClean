using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("department_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("department_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "department id"], Any[0, "name"], Any[0, "creation"], Any[0, "ranking"], Any[0, "budget in billions"], Any[0, "num employees"], Any[1, "head id"], Any[1, "name"], Any[1, "born state"], Any[1, "age"], Any[2, "department id"], Any[2, "head id"], Any[2, "temporary acting"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "department id"], Any[0, "name"], Any[0, "creation"], Any[0, "ranking"], Any[0, "budget in billions"], Any[0, "num employees"], Any[1, "head id"], Any[1, "name"], Any[1, "born state"], Any[1, "age"], Any[2, "department id"], Any[2, "head id"], Any[2, "temporary acting"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model DepartmentManagementModel begin
    @class Department begin
        department_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        creation ~ ChooseUniformly(possibilities[:creation])
        ranking ~ ChooseUniformly(possibilities[:ranking])
        budget_in_billions ~ ChooseUniformly(possibilities[:budget_in_billions])
        num_employees ~ ChooseUniformly(possibilities[:num_employees])
    end

    @class Head begin
        head_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        born_state ~ ChooseUniformly(possibilities[:born_state])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Management begin
        department_id ~ Unmodeled()
        head_id ~ ChooseUniformly(possibilities[:head_id])
        temporary_acting ~ ChooseUniformly(possibilities[:temporary_acting])
    end

    @class Obs begin
        department ~ Department
        head ~ Head
        management ~ Management
    end
end

query = @query DepartmentManagementModel.Obs [
    department_id department.department_id
    department_name department.name
    department_creation department.creation
    department_ranking department.ranking
    department_budget_in_billions department.budget_in_billions
    department_num_employees department.num_employees
    head_id head.head_id
    head_name head.name
    head_born_state head.born_state
    head_age head.age
    management_temporary_acting management.temporary_acting
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
