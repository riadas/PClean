using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("gymnast_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("gymnast_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "gymnast id"], Any[0, "floor exercise points"], Any[0, "pommel horse points"], Any[0, "rings points"], Any[0, "vault points"], Any[0, "parallel bars points"], Any[0, "horizontal bar points"], Any[0, "total points"], Any[1, "people id"], Any[1, "name"], Any[1, "age"], Any[1, "height"], Any[1, "hometown"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[1, 9]])
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







PClean.@model GymnastModel begin
    @class Gymnast begin
        floor_exercise_points ~ ChooseUniformly(possibilities[:floor_exercise_points])
        pommel_horse_points ~ ChooseUniformly(possibilities[:pommel_horse_points])
        rings_points ~ ChooseUniformly(possibilities[:rings_points])
        vault_points ~ ChooseUniformly(possibilities[:vault_points])
        parallel_bars_points ~ ChooseUniformly(possibilities[:parallel_bars_points])
        horizontal_bar_points ~ ChooseUniformly(possibilities[:horizontal_bar_points])
        total_points ~ ChooseUniformly(possibilities[:total_points])
    end

    @class People begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        height ~ ChooseUniformly(possibilities[:height])
        hometown ~ ChooseUniformly(possibilities[:hometown])
    end

    @class Obs begin
        gymnast ~ Gymnast
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
    people_id gymnast.people.people_id
    people_name gymnast.people.name
    people_age gymnast.people.age
    people_height gymnast.people.height
    people_hometown gymnast.people.hometown
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
