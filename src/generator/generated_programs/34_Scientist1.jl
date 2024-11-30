using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("scientists_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("scientists_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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

    @class Obs begin
        scientists ~ Scientists
        projects ~ Projects
    end
end

query = @query Scientist1Model.Obs [
    scientists_ssn scientists.ssn
    scientists_name scientists.name
    projects_code projects.code
    projects_name projects.name
    projects_hours projects.hours
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
