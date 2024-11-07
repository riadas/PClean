using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("gymnast_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("gymnast_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "gymnast id"], Any[0, "floor exercise points"], Any[0, "pommel horse points"], Any[0, "rings points"], Any[0, "vault points"], Any[0, "parallel bars points"], Any[0, "horizontal bar points"], Any[0, "total points"], Any[1, "people id"], Any[1, "name"], Any[1, "age"], Any[1, "height"], Any[1, "hometown"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "gymnast id"], Any[0, "floor exercise points"], Any[0, "pommel horse points"], Any[0, "rings points"], Any[0, "vault points"], Any[0, "parallel bars points"], Any[0, "horizontal bar points"], Any[0, "total points"], Any[1, "people id"], Any[1, "name"], Any[1, "age"], Any[1, "height"], Any[1, "hometown"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model GymnastModel begin
    @class Gymnast begin
        gymnast_id ~ Unmodeled()
        floor_exercise_points ~ ChooseUniformly(possibilities[:floor_exercise_points])
        pommel_horse_points ~ ChooseUniformly(possibilities[:pommel_horse_points])
        rings_points ~ ChooseUniformly(possibilities[:rings_points])
        vault_points ~ ChooseUniformly(possibilities[:vault_points])
        parallel_bars_points ~ ChooseUniformly(possibilities[:parallel_bars_points])
        horizontal_bar_points ~ ChooseUniformly(possibilities[:horizontal_bar_points])
        total_points ~ ChooseUniformly(possibilities[:total_points])
    end

    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        height ~ ChooseUniformly(possibilities[:height])
        hometown ~ ChooseUniformly(possibilities[:hometown])
    end

    @class Obs begin
        gymnast ~ Gymnast
        people ~ People
    end
end

query = @query GymnastModel.Obs [
    gymnast_floor_exercise_points gymnast.floor_exercise_points
    gymnast_pommel_horse_points gymnast.pommel_horse_points
    gymnast_rings_points gymnast.rings_points
    gymnast_vault_points gymnast.vault_points
    gymnast_parallel_bars_points gymnast.parallel_bars_points
    gymnast_horizontal_bar_points gymnast.horizontal_bar_points
    gymnast_total_points gymnast.total_points
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

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
