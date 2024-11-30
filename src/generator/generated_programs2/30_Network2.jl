using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("person_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("person_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[0, "gender"], Any[0, "job"], Any[1, "name"], Any[1, "friend"], Any[1, "year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[0, "gender"], Any[0, "job"], Any[1, "name"], Any[1, "friend"], Any[1, "year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["friend", "name"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "age"], Any[0, "city"], Any[0, "gender"], Any[0, "job"], Any[1, "year"]]
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





PClean.@model Network2Model begin
    @class Person begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        city ~ ChooseUniformly(possibilities[:city])
        gender ~ ChooseUniformly(possibilities[:gender])
        job ~ ChooseUniformly(possibilities[:job])
    end

    @class Person_Friend begin
        person ~ Person
        person ~ Person
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        person_Friend ~ Person_Friend
    end
end

query = @query Network2Model.Obs [
    person_name person_Friend.person.name
    person_age person_Friend.person.age
    person_city person_Friend.person.city
    person_gender person_Friend.person.gender
    person_job person_Friend.person.job
    person_friend_year person_Friend.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
