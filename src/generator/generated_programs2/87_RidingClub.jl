using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("player_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("player_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "player id"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club id"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "coach id"], Any[2, "player name"], Any[2, "gender"], Any[2, "club id"], Any[2, "rank"], Any[3, "player id"], Any[3, "coach id"], Any[3, "starting year"], Any[4, "rank"], Any[4, "club id"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "player id"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club id"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "coach id"], Any[2, "player name"], Any[2, "gender"], Any[2, "club id"], Any[2, "rank"], Any[3, "player id"], Any[3, "coach id"], Any[3, "starting year"], Any[4, "rank"], Any[4, "club id"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "player id"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club id"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "coach id"], Any[2, "player name"], Any[2, "gender"], Any[2, "club id"], Any[2, "rank"], Any[3, "player id"], Any[3, "coach id"], Any[3, "starting year"], Any[4, "rank"], Any[4, "club id"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "player id"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club id"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "coach id"], Any[2, "player name"], Any[2, "gender"], Any[2, "club id"], Any[2, "rank"], Any[3, "player id"], Any[3, "coach id"], Any[3, "starting year"], Any[4, "rank"], Any[4, "club id"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "player id"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club id"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "coach id"], Any[2, "player name"], Any[2, "gender"], Any[2, "club id"], Any[2, "rank"], Any[3, "player id"], Any[3, "coach id"], Any[3, "starting year"], Any[4, "rank"], Any[4, "club id"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[16, 9], Any[19, 13], Any[18, 1], Any[22, 9]])
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







PClean.@model RidingClubModel begin
    @class Player begin
        sponsor_name ~ ChooseUniformly(possibilities[:sponsor_name])
        player_name ~ ChooseUniformly(possibilities[:player_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        residence ~ ChooseUniformly(possibilities[:residence])
        occupation ~ ChooseUniformly(possibilities[:occupation])
        votes ~ ChooseUniformly(possibilities[:votes])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Club begin
        club_name ~ ChooseUniformly(possibilities[:club_name])
        region ~ ChooseUniformly(possibilities[:region])
        start_year ~ ChooseUniformly(possibilities[:start_year])
    end

    @class Coach begin
        player_name ~ ChooseUniformly(possibilities[:player_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        club ~ Club
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Player_coach begin
        coach ~ Coach
        starting_year ~ ChooseUniformly(possibilities[:starting_year])
    end

    @class Match_result begin
        rank ~ ChooseUniformly(possibilities[:rank])
        club ~ Club
        gold ~ ChooseUniformly(possibilities[:gold])
        big_silver ~ ChooseUniformly(possibilities[:big_silver])
        small_silver ~ ChooseUniformly(possibilities[:small_silver])
        bronze ~ ChooseUniformly(possibilities[:bronze])
        points ~ ChooseUniformly(possibilities[:points])
    end

    @class Obs begin
        player_coach ~ Player_coach
        match_result ~ Match_result
    end
end

query = @query RidingClubModel.Obs [
    player_id player_coach.player.player_id
    player_sponsor_name player_coach.player.sponsor_name
    player_name player_coach.player.player_name
    player_gender player_coach.player.gender
    player_residence player_coach.player.residence
    player_occupation player_coach.player.occupation
    player_votes player_coach.player.votes
    player_rank player_coach.player.rank
    club_id match_result.club.club_id
    club_name match_result.club.club_name
    club_region match_result.club.region
    club_start_year match_result.club.start_year
    coach_id player_coach.coach.coach_id
    coach_player_name player_coach.coach.player_name
    coach_gender player_coach.coach.gender
    coach_rank player_coach.coach.rank
    player_coach_starting_year player_coach.starting_year
    match_result_rank match_result.rank
    match_result_gold match_result.gold
    match_result_big_silver match_result.big_silver
    match_result_small_silver match_result.small_silver
    match_result_bronze match_result.bronze
    match_result_points match_result.points
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
