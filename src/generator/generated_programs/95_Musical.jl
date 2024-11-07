using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("musical_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("musical_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "musical id"], Any[0, "name"], Any[0, "year"], Any[0, "award"], Any[0, "category"], Any[0, "nominee"], Any[0, "result"], Any[1, "actor id"], Any[1, "name"], Any[1, "musical id"], Any[1, "character"], Any[1, "duration"], Any[1, "age"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "musical id"], Any[0, "name"], Any[0, "year"], Any[0, "award"], Any[0, "category"], Any[0, "nominee"], Any[0, "result"], Any[1, "actor id"], Any[1, "name"], Any[1, "musical id"], Any[1, "character"], Any[1, "duration"], Any[1, "age"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Actor begin
        actor_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        musical_id ~ ChooseUniformly(possibilities[:musical_id])
        character ~ ChooseUniformly(possibilities[:character])
        duration ~ ChooseUniformly(possibilities[:duration])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Obs begin
        musical ~ Musical
        actor ~ Actor
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
    actor_id actor.actor_id
    actor_name actor.name
    actor_character actor.character
    actor_duration actor.duration
    actor_age actor.age
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
