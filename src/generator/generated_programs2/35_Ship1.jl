using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("captain_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("captain_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "captain id"], Any[0, "name"], Any[0, "ship id"], Any[0, "age"], Any[0, "class"], Any[0, "rank"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "built year"], Any[1, "class"], Any[1, "flag"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "captain id"], Any[0, "name"], Any[0, "ship id"], Any[0, "age"], Any[0, "class"], Any[0, "rank"], Any[1, "ship id"], Any[1, "name"], Any[1, "type"], Any[1, "built year"], Any[1, "class"], Any[1, "flag"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["ship id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "captain id"], Any[0, "name"], Any[0, "age"], Any[0, "class"], Any[0, "rank"], Any[1, "name"], Any[1, "type"], Any[1, "built year"], Any[1, "class"], Any[1, "flag"]]
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





PClean.@model Ship1Model begin
    @class Ship begin
        ship_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        built_year ~ ChooseUniformly(possibilities[:built_year])
        class ~ ChooseUniformly(possibilities[:class])
        flag ~ ChooseUniformly(possibilities[:flag])
    end

    @class Captain begin
        captain_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        ship ~ Ship
        age ~ ChooseUniformly(possibilities[:age])
        class ~ ChooseUniformly(possibilities[:class])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Obs begin
        captain ~ Captain
    end
end

query = @query Ship1Model.Obs [
    captain_id captain.captain_id
    captain_name captain.name
    captain_age captain.age
    captain_class captain.class
    captain_rank captain.rank
    ship_id captain.ship.ship_id
    ship_name captain.ship.name
    ship_type captain.ship.type
    ship_built_year captain.ship.built_year
    ship_class captain.ship.class
    ship_flag captain.ship.flag
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
