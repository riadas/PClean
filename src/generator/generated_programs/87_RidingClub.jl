using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("player_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("player_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "player id"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club id"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "coach id"], Any[2, "player name"], Any[2, "gender"], Any[2, "club id"], Any[2, "rank"], Any[3, "player id"], Any[3, "coach id"], Any[3, "starting year"], Any[4, "rank"], Any[4, "club id"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "player id"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club id"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "coach id"], Any[2, "player name"], Any[2, "gender"], Any[2, "club id"], Any[2, "rank"], Any[3, "player id"], Any[3, "coach id"], Any[3, "starting year"], Any[4, "rank"], Any[4, "club id"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model RidingClubModel begin
    @class Player begin
        player_id ~ Unmodeled()
        sponsor_name ~ ChooseUniformly(possibilities[:sponsor_name])
        player_name ~ ChooseUniformly(possibilities[:player_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        residence ~ ChooseUniformly(possibilities[:residence])
        occupation ~ ChooseUniformly(possibilities[:occupation])
        votes ~ ChooseUniformly(possibilities[:votes])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Club begin
        club_id ~ Unmodeled()
        club_name ~ ChooseUniformly(possibilities[:club_name])
        region ~ ChooseUniformly(possibilities[:region])
        start_year ~ ChooseUniformly(possibilities[:start_year])
    end

    @class Coach begin
        coach_id ~ Unmodeled()
        player_name ~ ChooseUniformly(possibilities[:player_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        club_id ~ ChooseUniformly(possibilities[:club_id])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Player_Coach begin
        player_id ~ Unmodeled()
        coach_id ~ ChooseUniformly(possibilities[:coach_id])
        starting_year ~ ChooseUniformly(possibilities[:starting_year])
    end

    @class Match_Result begin
        rank ~ ChooseUniformly(possibilities[:rank])
        club_id ~ ChooseUniformly(possibilities[:club_id])
        gold ~ ChooseUniformly(possibilities[:gold])
        big_silver ~ ChooseUniformly(possibilities[:big_silver])
        small_silver ~ ChooseUniformly(possibilities[:small_silver])
        bronze ~ ChooseUniformly(possibilities[:bronze])
        points ~ ChooseUniformly(possibilities[:points])
    end

    @class Obs begin
        player ~ Player
        club ~ Club
        coach ~ Coach
        player_Coach ~ Player_Coach
        match_Result ~ Match_Result
    end
end

query = @query RidingClubModel.Obs [
    player_id player.player_id
    player_sponsor_name player.sponsor_name
    player_name player.player_name
    player_gender player.gender
    player_residence player.residence
    player_occupation player.occupation
    player_votes player.votes
    player_rank player.rank
    club_id club.club_id
    club_name club.club_name
    club_region club.region
    club_start_year club.start_year
    coach_id coach.coach_id
    coach_player_name coach.player_name
    coach_gender coach.gender
    coach_rank coach.rank
    player_coach_starting_year player_Coach.starting_year
    match_result_rank match_Result.rank
    match_result_gold match_Result.gold
    match_result_big_silver match_Result.big_silver
    match_result_small_silver match_Result.small_silver
    match_result_bronze match_Result.bronze
    match_result_points match_Result.points
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
