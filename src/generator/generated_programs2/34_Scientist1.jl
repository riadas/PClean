using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("scientists_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("scientists_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "ssn"], Any[0, "name"], Any[1, "code"], Any[1, "name"], Any[1, "hours"], Any[2, "scientist"], Any[2, "project"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "ssn"], Any[0, "name"], Any[1, "code"], Any[1, "name"], Any[1, "hours"], Any[2, "scientist"], Any[2, "project"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["project", "scientist"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "ssn"], Any[0, "name"], Any[1, "code"], Any[1, "name"], Any[1, "hours"]]
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





PClean.@model Scientist1Model begin
    @class Scientists begin
        ssn ~ ChooseUniformly(possibilities[:ssn])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Projects begin
        code ~ ChooseUniformly(possibilities[:code])
        name ~ ChooseUniformly(possibilities[:name])
        hours ~ ChooseUniformly(possibilities[:hours])
    end

    @class Assigned_To begin
        scientists ~ Scientists
        projects ~ Projects
    end

    @class Obs begin
        assigned_To ~ Assigned_To
    end
end

query = @query Scientist1Model.Obs [
    scientists_ssn assigned_To.scientists.ssn
    scientists_name assigned_To.scientists.name
    projects_code assigned_To.projects.code
    projects_name assigned_To.projects.name
    projects_hours assigned_To.projects.hours
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
