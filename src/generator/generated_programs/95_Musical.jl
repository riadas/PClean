using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("musical_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("musical_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "musical id"], Any[0, "name"], Any[0, "year"], Any[0, "award"], Any[0, "category"], Any[0, "nominee"], Any[0, "result"], Any[1, "actor id"], Any[1, "name"], Any[1, "musical id"], Any[1, "character"], Any[1, "duration"], Any[1, "age"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "musical id"], Any[0, "name"], Any[0, "year"], Any[0, "award"], Any[0, "category"], Any[0, "nominee"], Any[0, "result"], Any[1, "actor id"], Any[1, "name"], Any[1, "musical id"], Any[1, "character"], Any[1, "duration"], Any[1, "age"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["musical id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "year"], Any[0, "award"], Any[0, "category"], Any[0, "nominee"], Any[0, "result"], Any[1, "actor id"], Any[1, "name"], Any[1, "character"], Any[1, "duration"], Any[1, "age"]]
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





PClean.@model MusicalModel begin
    @class Musical begin
        musical_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        year ~ ChooseUniformly(possibilities[:year])
        award ~ ChooseUniformly(possibilities[:award])
        category ~ ChooseUniformly(possibilities[:category])
        nominee ~ ChooseUniformly(possibilities[:nominee])
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Obs begin
        musical ~ Musical
        actor_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        character ~ ChooseUniformly(possibilities[:character])
        duration ~ ChooseUniformly(possibilities[:duration])
        age ~ ChooseUniformly(possibilities[:age])
    end
end

query = @query MusicalModel.Obs [
    musical_id musical.musical_id
    musical_name musical.name
    musical_year musical.year
    musical_award musical.award
    musical_category musical.category
    musical_nominee musical.nominee
    musical_result musical.result
    actor_id actor_id
    actor_name name
    actor_character character
    actor_duration duration
    actor_age age
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
