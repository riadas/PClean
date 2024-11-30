using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("gymnast_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("gymnast_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "gymnast id"], Any[0, "floor exercise points"], Any[0, "pommel horse points"], Any[0, "rings points"], Any[0, "vault points"], Any[0, "parallel bars points"], Any[0, "horizontal bar points"], Any[0, "total points"], Any[1, "people id"], Any[1, "name"], Any[1, "age"], Any[1, "height"], Any[1, "hometown"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "gymnast id"], Any[0, "floor exercise points"], Any[0, "pommel horse points"], Any[0, "rings points"], Any[0, "vault points"], Any[0, "parallel bars points"], Any[0, "horizontal bar points"], Any[0, "total points"], Any[1, "people id"], Any[1, "name"], Any[1, "age"], Any[1, "height"], Any[1, "hometown"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["gymnast id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "floor exercise points"], Any[0, "pommel horse points"], Any[0, "rings points"], Any[0, "vault points"], Any[0, "parallel bars points"], Any[0, "horizontal bar points"], Any[0, "total points"], Any[1, "people id"], Any[1, "name"], Any[1, "age"], Any[1, "height"], Any[1, "hometown"]]
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





PClean.@model GymnastModel begin
    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        height ~ ChooseUniformly(possibilities[:height])
        hometown ~ ChooseUniformly(possibilities[:hometown])
    end

    @class Obs begin
        people ~ People
        floor_exercise_points ~ ChooseUniformly(possibilities[:floor_exercise_points])
        pommel_horse_points ~ ChooseUniformly(possibilities[:pommel_horse_points])
        rings_points ~ ChooseUniformly(possibilities[:rings_points])
        vault_points ~ ChooseUniformly(possibilities[:vault_points])
        parallel_bars_points ~ ChooseUniformly(possibilities[:parallel_bars_points])
        horizontal_bar_points ~ ChooseUniformly(possibilities[:horizontal_bar_points])
        total_points ~ ChooseUniformly(possibilities[:total_points])
    end
end

query = @query GymnastModel.Obs [
    gymnast_floor_exercise_points floor_exercise_points
    gymnast_pommel_horse_points pommel_horse_points
    gymnast_rings_points rings_points
    gymnast_vault_points vault_points
    gymnast_parallel_bars_points parallel_bars_points
    gymnast_horizontal_bar_points horizontal_bar_points
    gymnast_total_points total_points
    people_id people.people_id
    people_name people.name
    people_age people.age
    people_height people.height
    people_hometown people.hometown
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
