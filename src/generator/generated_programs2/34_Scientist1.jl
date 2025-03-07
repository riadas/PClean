using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("scientists_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("scientists_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "ssn"], Any[0, "name"], Any[1, "code"], Any[1, "name"], Any[1, "hours"], Any[2, "scientist"], Any[2, "project"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 3], Any[6, 1]])
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

    @class Assigned_to begin
        scientists ~ Scientists
        projects ~ Projects
    end

    @class Obs begin
        assigned_to ~ Assigned_to
    end
end

query = @query Scientist1Model.Obs [
    scientists_ssn assigned_to.scientists.ssn
    scientists_name assigned_to.scientists.name
    projects_code assigned_to.projects.code
    projects_name assigned_to.projects.name
    projects_hours assigned_to.projects.hours
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
