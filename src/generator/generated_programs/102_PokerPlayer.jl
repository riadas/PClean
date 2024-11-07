using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("poker player_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("poker player_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "poker player id"], Any[0, "people id"], Any[0, "final table made"], Any[0, "best finish"], Any[0, "money rank"], Any[0, "earnings"], Any[1, "people id"], Any[1, "nationality"], Any[1, "name"], Any[1, "birth date"], Any[1, "height"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "poker player id"], Any[0, "people id"], Any[0, "final table made"], Any[0, "best finish"], Any[0, "money rank"], Any[0, "earnings"], Any[1, "people id"], Any[1, "nationality"], Any[1, "name"], Any[1, "birth date"], Any[1, "height"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model PokerPlayerModel begin
    @class Poker_Player begin
        poker_player_id ~ Unmodeled()
        people_id ~ ChooseUniformly(possibilities[:people_id])
        final_table_made ~ ChooseUniformly(possibilities[:final_table_made])
        best_finish ~ ChooseUniformly(possibilities[:best_finish])
        money_rank ~ ChooseUniformly(possibilities[:money_rank])
        earnings ~ ChooseUniformly(possibilities[:earnings])
    end

    @class People begin
        people_id ~ Unmodeled()
        nationality ~ ChooseUniformly(possibilities[:nationality])
        name ~ ChooseUniformly(possibilities[:name])
        birth_date ~ ChooseUniformly(possibilities[:birth_date])
        height ~ ChooseUniformly(possibilities[:height])
    end

    @class Obs begin
        poker_Player ~ Poker_Player
        people ~ People
    end
end

query = @query PokerPlayerModel.Obs [
    poker_player_id poker_Player.poker_player_id
    poker_player_final_table_made poker_Player.final_table_made
    poker_player_best_finish poker_Player.best_finish
    poker_player_money_rank poker_Player.money_rank
    poker_player_earnings poker_Player.earnings
    people_id people.people_id
    people_nationality people.nationality
    people_name people.name
    people_birth_date people.birth_date
    people_height people.height
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
