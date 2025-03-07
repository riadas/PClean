using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("body_builder_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("body_builder_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "body builder id"], Any[0, "people id"], Any[0, "snatch"], Any[0, "clean jerk"], Any[0, "total"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "birth date"], Any[1, "birth place"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[2, 6]])
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







PClean.@model BodyBuilderModel begin
    @class People begin
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
        birth_date ~ ChooseUniformly(possibilities[:birth_date])
        birth_place ~ ChooseUniformly(possibilities[:birth_place])
    end

    @class Body_builder begin
        people ~ People
        snatch ~ ChooseUniformly(possibilities[:snatch])
        clean_jerk ~ ChooseUniformly(possibilities[:clean_jerk])
        total ~ ChooseUniformly(possibilities[:total])
    end

    @class Obs begin
        body_builder ~ Body_builder
    end
end

query = @query BodyBuilderModel.Obs [
    body_builder_id body_builder.body_builder_id
    body_builder_snatch body_builder.snatch
    body_builder_clean_jerk body_builder.clean_jerk
    body_builder_total body_builder.total
    people_id body_builder.people.people_id
    people_name body_builder.people.name
    people_height body_builder.people.height
    people_weight body_builder.people.weight
    people_birth_date body_builder.people.birth_date
    people_birth_place body_builder.people.birth_place
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
