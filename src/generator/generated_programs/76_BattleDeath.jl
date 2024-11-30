using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("battle_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("battle_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "date"], Any[0, "bulgarian commander"], Any[0, "latin commander"], Any[0, "result"], Any[1, "lost in battle"], Any[1, "id"], Any[1, "name"], Any[1, "tonnage"], Any[1, "ship type"], Any[1, "location"], Any[1, "disposition of ship"], Any[2, "caused by ship id"], Any[2, "id"], Any[2, "note"], Any[2, "killed"], Any[2, "injured"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "date"], Any[0, "bulgarian commander"], Any[0, "latin commander"], Any[0, "result"], Any[1, "lost in battle"], Any[1, "id"], Any[1, "name"], Any[1, "tonnage"], Any[1, "ship type"], Any[1, "location"], Any[1, "disposition of ship"], Any[2, "caused by ship id"], Any[2, "id"], Any[2, "note"], Any[2, "killed"], Any[2, "injured"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["lost in battle", "caused by ship id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "date"], Any[0, "bulgarian commander"], Any[0, "latin commander"], Any[0, "result"], Any[1, "id"], Any[1, "name"], Any[1, "tonnage"], Any[1, "ship type"], Any[1, "location"], Any[1, "disposition of ship"], Any[2, "id"], Any[2, "note"], Any[2, "killed"], Any[2, "injured"]]
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





PClean.@model BattleDeathModel begin
    @class Battle begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        date ~ ChooseUniformly(possibilities[:date])
        bulgarian_commander ~ ChooseUniformly(possibilities[:bulgarian_commander])
        latin_commander ~ ChooseUniformly(possibilities[:latin_commander])
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Obs begin
        battle ~ Battle
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        tonnage ~ ChooseUniformly(possibilities[:tonnage])
        ship_type ~ ChooseUniformly(possibilities[:ship_type])
        location ~ ChooseUniformly(possibilities[:location])
        disposition_of_ship ~ ChooseUniformly(possibilities[:disposition_of_ship])
        id ~ ChooseUniformly(possibilities[:id])
        note ~ ChooseUniformly(possibilities[:note])
        killed ~ ChooseUniformly(possibilities[:killed])
        injured ~ ChooseUniformly(possibilities[:injured])
    end
end

query = @query BattleDeathModel.Obs [
    battle_id battle.id
    battle_name battle.name
    battle_date battle.date
    battle_bulgarian_commander battle.bulgarian_commander
    battle_latin_commander battle.latin_commander
    battle_result battle.result
    ship_id id
    ship_name name
    ship_tonnage tonnage
    ship_type ship_type
    ship_location location
    disposition_of_ship disposition_of_ship
    death_id id
    death_note note
    death_killed killed
    death_injured injured
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
