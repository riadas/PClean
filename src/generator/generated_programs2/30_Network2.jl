using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("person_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("person_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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
cols = Any[Any[-1, "*"], Any[0, "name"], Any[0, "age"], Any[0, "city"], Any[0, "gender"], Any[0, "job"], Any[1, "name"], Any[1, "friend"], Any[1, "year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[6, 1]])
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







PClean.@model Network2Model begin
    @class Person begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        city ~ ChooseUniformly(possibilities[:city])
        gender ~ ChooseUniformly(possibilities[:gender])
        job ~ ChooseUniformly(possibilities[:job])
    end

    @class Person_friend begin
        person ~ Person
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        person_friend ~ Person_friend
    end
end

query = @query Network2Model.Obs [
    person_name person_friend.person.name
    person_age person_friend.person.age
    person_city person_friend.person.city
    person_gender person_friend.person.gender
    person_job person_friend.person.job
    person_friend_year person_friend.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
