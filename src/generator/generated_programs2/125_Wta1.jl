using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("players_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("players_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "player id"], Any[0, "first name"], Any[0, "last name"], Any[0, "hand"], Any[0, "birth date"], Any[0, "country code"], Any[1, "best of"], Any[1, "draw size"], Any[1, "loser age"], Any[1, "loser entry"], Any[1, "loser hand"], Any[1, "loser ht"], Any[1, "loser id"], Any[1, "loser ioc"], Any[1, "loser name"], Any[1, "loser rank"], Any[1, "loser rank points"], Any[1, "loser seed"], Any[1, "match num"], Any[1, "minutes"], Any[1, "round"], Any[1, "score"], Any[1, "surface"], Any[1, "tourney date"], Any[1, "tourney id"], Any[1, "tourney level"], Any[1, "tourney name"], Any[1, "winner age"], Any[1, "winner entry"], Any[1, "winner hand"], Any[1, "winner ht"], Any[1, "winner id"], Any[1, "winner ioc"], Any[1, "winner name"], Any[1, "winner rank"], Any[1, "winner rank points"], Any[1, "winner seed"], Any[1, "year"], Any[2, "ranking date"], Any[2, "ranking"], Any[2, "player id"], Any[2, "ranking points"], Any[2, "tours"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "player id"], Any[0, "first name"], Any[0, "last name"], Any[0, "hand"], Any[0, "birth date"], Any[0, "country code"], Any[1, "best of"], Any[1, "draw size"], Any[1, "loser age"], Any[1, "loser entry"], Any[1, "loser hand"], Any[1, "loser ht"], Any[1, "loser id"], Any[1, "loser ioc"], Any[1, "loser name"], Any[1, "loser rank"], Any[1, "loser rank points"], Any[1, "loser seed"], Any[1, "match num"], Any[1, "minutes"], Any[1, "round"], Any[1, "score"], Any[1, "surface"], Any[1, "tourney date"], Any[1, "tourney id"], Any[1, "tourney level"], Any[1, "tourney name"], Any[1, "winner age"], Any[1, "winner entry"], Any[1, "winner hand"], Any[1, "winner ht"], Any[1, "winner id"], Any[1, "winner ioc"], Any[1, "winner name"], Any[1, "winner rank"], Any[1, "winner rank points"], Any[1, "winner seed"], Any[1, "year"], Any[2, "ranking date"], Any[2, "ranking"], Any[2, "player id"], Any[2, "ranking points"], Any[2, "tours"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "player id"], Any[0, "first name"], Any[0, "last name"], Any[0, "hand"], Any[0, "birth date"], Any[0, "country code"], Any[1, "best of"], Any[1, "draw size"], Any[1, "loser age"], Any[1, "loser entry"], Any[1, "loser hand"], Any[1, "loser ht"], Any[1, "loser id"], Any[1, "loser ioc"], Any[1, "loser name"], Any[1, "loser rank"], Any[1, "loser rank points"], Any[1, "loser seed"], Any[1, "match num"], Any[1, "minutes"], Any[1, "round"], Any[1, "score"], Any[1, "surface"], Any[1, "tourney date"], Any[1, "tourney id"], Any[1, "tourney level"], Any[1, "tourney name"], Any[1, "winner age"], Any[1, "winner entry"], Any[1, "winner hand"], Any[1, "winner ht"], Any[1, "winner id"], Any[1, "winner ioc"], Any[1, "winner name"], Any[1, "winner rank"], Any[1, "winner rank points"], Any[1, "winner seed"], Any[1, "year"], Any[2, "ranking date"], Any[2, "ranking"], Any[2, "player id"], Any[2, "ranking points"], Any[2, "tours"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "player id"], Any[0, "first name"], Any[0, "last name"], Any[0, "hand"], Any[0, "birth date"], Any[0, "country code"], Any[1, "best of"], Any[1, "draw size"], Any[1, "loser age"], Any[1, "loser entry"], Any[1, "loser hand"], Any[1, "loser ht"], Any[1, "loser id"], Any[1, "loser ioc"], Any[1, "loser name"], Any[1, "loser rank"], Any[1, "loser rank points"], Any[1, "loser seed"], Any[1, "match num"], Any[1, "minutes"], Any[1, "round"], Any[1, "score"], Any[1, "surface"], Any[1, "tourney date"], Any[1, "tourney id"], Any[1, "tourney level"], Any[1, "tourney name"], Any[1, "winner age"], Any[1, "winner entry"], Any[1, "winner hand"], Any[1, "winner ht"], Any[1, "winner id"], Any[1, "winner ioc"], Any[1, "winner name"], Any[1, "winner rank"], Any[1, "winner rank points"], Any[1, "winner seed"], Any[1, "year"], Any[2, "ranking date"], Any[2, "ranking"], Any[2, "player id"], Any[2, "ranking points"], Any[2, "tours"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "player id"], Any[0, "first name"], Any[0, "last name"], Any[0, "hand"], Any[0, "birth date"], Any[0, "country code"], Any[1, "best of"], Any[1, "draw size"], Any[1, "loser age"], Any[1, "loser entry"], Any[1, "loser hand"], Any[1, "loser ht"], Any[1, "loser id"], Any[1, "loser ioc"], Any[1, "loser name"], Any[1, "loser rank"], Any[1, "loser rank points"], Any[1, "loser seed"], Any[1, "match num"], Any[1, "minutes"], Any[1, "round"], Any[1, "score"], Any[1, "surface"], Any[1, "tourney date"], Any[1, "tourney id"], Any[1, "tourney level"], Any[1, "tourney name"], Any[1, "winner age"], Any[1, "winner entry"], Any[1, "winner hand"], Any[1, "winner ht"], Any[1, "winner id"], Any[1, "winner ioc"], Any[1, "winner name"], Any[1, "winner rank"], Any[1, "winner rank points"], Any[1, "winner seed"], Any[1, "year"], Any[2, "ranking date"], Any[2, "ranking"], Any[2, "player id"], Any[2, "ranking points"], Any[2, "tours"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[32, 1], Any[13, 1], Any[41, 1]])
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







PClean.@model Wta1Model begin
    @class Players begin
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        hand ~ ChooseUniformly(possibilities[:hand])
        birth_date ~ TimePrior(possibilities[:birth_date])
        country_code ~ ChooseUniformly(possibilities[:country_code])
    end

    @class Matches begin
        best_of ~ ChooseUniformly(possibilities[:best_of])
        draw_size ~ ChooseUniformly(possibilities[:draw_size])
        loser_age ~ ChooseUniformly(possibilities[:loser_age])
        loser_entry ~ ChooseUniformly(possibilities[:loser_entry])
        loser_hand ~ ChooseUniformly(possibilities[:loser_hand])
        loser_ht ~ ChooseUniformly(possibilities[:loser_ht])
        players ~ Players
        loser_ioc ~ ChooseUniformly(possibilities[:loser_ioc])
        loser_name ~ ChooseUniformly(possibilities[:loser_name])
        loser_rank ~ ChooseUniformly(possibilities[:loser_rank])
        loser_rank_points ~ ChooseUniformly(possibilities[:loser_rank_points])
        loser_seed ~ ChooseUniformly(possibilities[:loser_seed])
        match_num ~ ChooseUniformly(possibilities[:match_num])
        minutes ~ ChooseUniformly(possibilities[:minutes])
        round ~ ChooseUniformly(possibilities[:round])
        score ~ ChooseUniformly(possibilities[:score])
        surface ~ ChooseUniformly(possibilities[:surface])
        tourney_date ~ TimePrior(possibilities[:tourney_date])
        tourney_id ~ ChooseUniformly(possibilities[:tourney_id])
        tourney_level ~ ChooseUniformly(possibilities[:tourney_level])
        tourney_name ~ ChooseUniformly(possibilities[:tourney_name])
        winner_age ~ ChooseUniformly(possibilities[:winner_age])
        winner_entry ~ ChooseUniformly(possibilities[:winner_entry])
        winner_hand ~ ChooseUniformly(possibilities[:winner_hand])
        winner_ht ~ ChooseUniformly(possibilities[:winner_ht])
        winner_ioc ~ ChooseUniformly(possibilities[:winner_ioc])
        winner_name ~ ChooseUniformly(possibilities[:winner_name])
        winner_rank ~ ChooseUniformly(possibilities[:winner_rank])
        winner_rank_points ~ ChooseUniformly(possibilities[:winner_rank_points])
        winner_seed ~ ChooseUniformly(possibilities[:winner_seed])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Rankings begin
        ranking_date ~ TimePrior(possibilities[:ranking_date])
        ranking ~ ChooseUniformly(possibilities[:ranking])
        players ~ Players
        ranking_points ~ ChooseUniformly(possibilities[:ranking_points])
        tours ~ ChooseUniformly(possibilities[:tours])
    end

    @class Obs begin
        matches ~ Matches
        rankings ~ Rankings
    end
end

query = @query Wta1Model.Obs [
    players_player_id matches.players.player_id
    players_first_name matches.players.first_name
    players_last_name matches.players.last_name
    players_hand matches.players.hand
    players_birth_date matches.players.birth_date
    players_country_code matches.players.country_code
    matches_best_of matches.best_of
    matches_draw_size matches.draw_size
    matches_loser_age matches.loser_age
    matches_loser_entry matches.loser_entry
    matches_loser_hand matches.loser_hand
    matches_loser_ht matches.loser_ht
    matches_loser_ioc matches.loser_ioc
    matches_loser_name matches.loser_name
    matches_loser_rank matches.loser_rank
    matches_loser_rank_points matches.loser_rank_points
    matches_loser_seed matches.loser_seed
    matches_match_num matches.match_num
    matches_minutes matches.minutes
    matches_round matches.round
    matches_score matches.score
    matches_surface matches.surface
    matches_tourney_date matches.tourney_date
    matches_tourney_id matches.tourney_id
    matches_tourney_level matches.tourney_level
    matches_tourney_name matches.tourney_name
    matches_winner_age matches.winner_age
    matches_winner_entry matches.winner_entry
    matches_winner_hand matches.winner_hand
    matches_winner_ht matches.winner_ht
    matches_winner_ioc matches.winner_ioc
    matches_winner_name matches.winner_name
    matches_winner_rank matches.winner_rank
    matches_winner_rank_points matches.winner_rank_points
    matches_winner_seed matches.winner_seed
    matches_year matches.year
    rankings_ranking_date rankings.ranking_date
    rankings_ranking rankings.ranking
    rankings_ranking_points rankings.ranking_points
    rankings_tours rankings.tours
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
