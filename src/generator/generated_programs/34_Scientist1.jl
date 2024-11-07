using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("scientists_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("scientists_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "ssn"], Any[0, "name"], Any[1, "code"], Any[1, "name"], Any[1, "hours"], Any[2, "scientist"], Any[2, "project"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "ssn"], Any[0, "name"], Any[1, "code"], Any[1, "name"], Any[1, "hours"], Any[2, "scientist"], Any[2, "project"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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
        scientist ~ ChooseUniformly(possibilities[:scientist])
        project ~ ChooseUniformly(possibilities[:project])
    end

    @class Obs begin
        scientists ~ Scientists
        projects ~ Projects
        assigned_To ~ Assigned_To
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

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
