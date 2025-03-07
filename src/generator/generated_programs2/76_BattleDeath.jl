using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("battle_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("battle_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "date"], Any[0, "bulgarian commander"], Any[0, "latin commander"], Any[0, "result"], Any[1, "lost in battle"], Any[1, "id"], Any[1, "name"], Any[1, "tonnage"], Any[1, "ship type"], Any[1, "location"], Any[1, "disposition of ship"], Any[2, "caused by ship id"], Any[2, "id"], Any[2, "note"], Any[2, "killed"], Any[2, "injured"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[14, 8]])
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







PClean.@model BattleDeathModel begin
    @class Battle begin
        name ~ ChooseUniformly(possibilities[:name])
        date ~ ChooseUniformly(possibilities[:date])
        bulgarian_commander ~ ChooseUniformly(possibilities[:bulgarian_commander])
        latin_commander ~ ChooseUniformly(possibilities[:latin_commander])
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Ship begin
        battle ~ Battle
        name ~ ChooseUniformly(possibilities[:name])
        tonnage ~ ChooseUniformly(possibilities[:tonnage])
        ship_type ~ ChooseUniformly(possibilities[:ship_type])
        location ~ ChooseUniformly(possibilities[:location])
        disposition_of_ship ~ ChooseUniformly(possibilities[:disposition_of_ship])
    end

    @class Death begin
        ship ~ Ship
        note ~ ChooseUniformly(possibilities[:note])
        killed ~ ChooseUniformly(possibilities[:killed])
        injured ~ ChooseUniformly(possibilities[:injured])
    end

    @class Obs begin
        death ~ Death
    end
end

query = @query BattleDeathModel.Obs [
    battle_name death.ship.battle.name
    battle_date death.ship.battle.date
    battle_bulgarian_commander death.ship.battle.bulgarian_commander
    battle_latin_commander death.ship.battle.latin_commander
    battle_result death.ship.battle.result
    ship_name death.ship.name
    ship_tonnage death.ship.tonnage
    ship_type death.ship.ship_type
    ship_location death.ship.location
    disposition_of_ship death.ship.disposition_of_ship
    death_note death.note
    death_killed death.killed
    death_injured death.injured
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
