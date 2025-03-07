using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("poker_player_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("poker_player_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "poker player id"], Any[0, "people id"], Any[0, "final table made"], Any[0, "best finish"], Any[0, "money rank"], Any[0, "earnings"], Any[1, "people id"], Any[1, "nationality"], Any[1, "name"], Any[1, "birth date"], Any[1, "height"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "poker player id"], Any[0, "people id"], Any[0, "final table made"], Any[0, "best finish"], Any[0, "money rank"], Any[0, "earnings"], Any[1, "people id"], Any[1, "nationality"], Any[1, "name"], Any[1, "birth date"], Any[1, "height"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "poker player id"], Any[0, "people id"], Any[0, "final table made"], Any[0, "best finish"], Any[0, "money rank"], Any[0, "earnings"], Any[1, "people id"], Any[1, "nationality"], Any[1, "name"], Any[1, "birth date"], Any[1, "height"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "poker player id"], Any[0, "people id"], Any[0, "final table made"], Any[0, "best finish"], Any[0, "money rank"], Any[0, "earnings"], Any[1, "people id"], Any[1, "nationality"], Any[1, "name"], Any[1, "birth date"], Any[1, "height"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "poker player id"], Any[0, "people id"], Any[0, "final table made"], Any[0, "best finish"], Any[0, "money rank"], Any[0, "earnings"], Any[1, "people id"], Any[1, "nationality"], Any[1, "name"], Any[1, "birth date"], Any[1, "height"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[2, 7]])
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







PClean.@model PokerPlayerModel begin
    @class People begin
        nationality ~ ChooseUniformly(possibilities[:nationality])
        name ~ ChooseUniformly(possibilities[:name])
        birth_date ~ ChooseUniformly(possibilities[:birth_date])
        height ~ ChooseUniformly(possibilities[:height])
    end

    @class Poker_player begin
        people ~ People
        final_table_made ~ ChooseUniformly(possibilities[:final_table_made])
        best_finish ~ ChooseUniformly(possibilities[:best_finish])
        money_rank ~ ChooseUniformly(possibilities[:money_rank])
        earnings ~ ChooseUniformly(possibilities[:earnings])
    end

    @class Obs begin
        poker_player ~ Poker_player
    end
end

query = @query PokerPlayerModel.Obs [
    poker_player_id poker_player.poker_player_id
    poker_player_final_table_made poker_player.final_table_made
    poker_player_best_finish poker_player.best_finish
    poker_player_money_rank poker_player.money_rank
    poker_player_earnings poker_player.earnings
    people_id poker_player.people.people_id
    people_nationality poker_player.people.nationality
    people_name poker_player.people.name
    people_birth_date poker_player.people.birth_date
    people_height poker_player.people.height
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
