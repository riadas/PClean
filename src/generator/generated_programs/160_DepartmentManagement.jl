using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("department_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("department_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "department id"], Any[0, "name"], Any[0, "creation"], Any[0, "ranking"], Any[0, "budget in billions"], Any[0, "num employees"], Any[1, "head id"], Any[1, "name"], Any[1, "born state"], Any[1, "age"], Any[2, "department id"], Any[2, "head id"], Any[2, "temporary acting"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "department id"], Any[0, "name"], Any[0, "creation"], Any[0, "ranking"], Any[0, "budget in billions"], Any[0, "num employees"], Any[1, "head id"], Any[1, "name"], Any[1, "born state"], Any[1, "age"], Any[2, "department id"], Any[2, "head id"], Any[2, "temporary acting"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["head id", "department id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "creation"], Any[0, "ranking"], Any[0, "budget in billions"], Any[0, "num employees"], Any[1, "name"], Any[1, "born state"], Any[1, "age"], Any[2, "temporary acting"]]
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

    @class Obs begin
        department ~ Department
        head ~ Head
        temporary_acting ~ ChooseUniformly(possibilities[:temporary_acting])
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
    management_temporary_acting temporary_acting
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
