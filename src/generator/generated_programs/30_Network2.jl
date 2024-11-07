using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("person_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("person_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[0, "gender"], Any[0, "job"], Any[1, "name"], Any[1, "friend"], Any[1, "year"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[0, "gender"], Any[0, "job"], Any[1, "name"], Any[1, "friend"], Any[1, "year"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Network2Model begin
    @class Person begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        city ~ ChooseUniformly(possibilities[:city])
        gender ~ ChooseUniformly(possibilities[:gender])
        job ~ ChooseUniformly(possibilities[:job])
    end

    @class Person_Friend begin
        name ~ ChooseUniformly(possibilities[:name])
        friend ~ ChooseUniformly(possibilities[:friend])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        person ~ Person
        person_Friend ~ Person_Friend
    end
end

query = @query Network2Model.Obs [
    person_name person.name
    person_age person.age
    person_city person.city
    person_gender person.gender
    person_job person.job
    person_friend_year person_Friend.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
