using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("body builder_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("body builder_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "body builder id"], Any[0, "people id"], Any[0, "snatch"], Any[0, "clean jerk"], Any[0, "total"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "birth date"], Any[1, "birth place"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "body builder id"], Any[0, "people id"], Any[0, "snatch"], Any[0, "clean jerk"], Any[0, "total"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "birth date"], Any[1, "birth place"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["people id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "body builder id"], Any[0, "snatch"], Any[0, "clean jerk"], Any[0, "total"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "birth date"], Any[1, "birth place"]]
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





PClean.@model BodyBuilderModel begin
    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
        birth_date ~ ChooseUniformly(possibilities[:birth_date])
        birth_place ~ ChooseUniformly(possibilities[:birth_place])
    end

    @class Obs begin
        people ~ People
        body_builder_id ~ Unmodeled()
        snatch ~ ChooseUniformly(possibilities[:snatch])
        clean_jerk ~ ChooseUniformly(possibilities[:clean_jerk])
        total ~ ChooseUniformly(possibilities[:total])
    end
end

query = @query BodyBuilderModel.Obs [
    body_builder_id body_builder_id
    body_builder_snatch snatch
    body_builder_clean_jerk clean_jerk
    body_builder_total total
    people_id people.people_id
    people_name people.name
    people_height people.height
    people_weight people.weight
    people_birth_date people.birth_date
    people_birth_place people.birth_place
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
