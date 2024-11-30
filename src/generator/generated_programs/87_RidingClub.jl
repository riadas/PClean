using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("player_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("player_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["club id", "coach id", "player id", "club id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "sponsor name"], Any[0, "player name"], Any[0, "gender"], Any[0, "residence"], Any[0, "occupation"], Any[0, "votes"], Any[0, "rank"], Any[1, "club name"], Any[1, "region"], Any[1, "start year"], Any[2, "player name"], Any[2, "gender"], Any[2, "rank"], Any[3, "starting year"], Any[4, "rank"], Any[4, "gold"], Any[4, "big silver"], Any[4, "small silver"], Any[4, "bronze"], Any[4, "points"]]
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

    @class Obs begin
        player ~ Player
        club ~ Club
        coach_id ~ Unmodeled()
        player_name ~ ChooseUniformly(possibilities[:player_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        rank ~ ChooseUniformly(possibilities[:rank])
        starting_year ~ ChooseUniformly(possibilities[:starting_year])
        rank ~ ChooseUniformly(possibilities[:rank])
        gold ~ ChooseUniformly(possibilities[:gold])
        big_silver ~ ChooseUniformly(possibilities[:big_silver])
        small_silver ~ ChooseUniformly(possibilities[:small_silver])
        bronze ~ ChooseUniformly(possibilities[:bronze])
        points ~ ChooseUniformly(possibilities[:points])
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
    coach_id coach_id
    coach_player_name player_name
    coach_gender gender
    coach_rank rank
    player_coach_starting_year starting_year
    match_result_rank rank
    match_result_gold gold
    match_result_big_silver big_silver
    match_result_small_silver small_silver
    match_result_bronze bronze
    match_result_points points
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
