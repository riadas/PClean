using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("body builder_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("body builder_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "body builder id"], Any[0, "people id"], Any[0, "snatch"], Any[0, "clean jerk"], Any[0, "total"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "birth date"], Any[1, "birth place"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "body builder id"], Any[0, "people id"], Any[0, "snatch"], Any[0, "clean jerk"], Any[0, "total"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "birth date"], Any[1, "birth place"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model BodyBuilderModel begin
    @class Body_Builder begin
        body_builder_id ~ Unmodeled()
        people_id ~ ChooseUniformly(possibilities[:people_id])
        snatch ~ ChooseUniformly(possibilities[:snatch])
        clean_jerk ~ ChooseUniformly(possibilities[:clean_jerk])
        total ~ ChooseUniformly(possibilities[:total])
    end

    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
        birth_date ~ ChooseUniformly(possibilities[:birth_date])
        birth_place ~ ChooseUniformly(possibilities[:birth_place])
    end

    @class Obs begin
        body_Builder ~ Body_Builder
        people ~ People
    end
end

query = @query BodyBuilderModel.Obs [
    body_builder_id body_Builder.body_builder_id
    body_builder_snatch body_Builder.snatch
    body_builder_clean_jerk body_Builder.clean_jerk
    body_builder_total body_Builder.total
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

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
