using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("stadium_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("stadium_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "stadium id"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "game id"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "stadium id"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "game id"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "stadium id"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "game id"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "stadium id"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "game id"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "stadium id"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "game id"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[15, 8]])
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







PClean.@model GameInjuryModel begin
    @class Stadium begin
        name ~ ChooseUniformly(possibilities[:name])
        home_games ~ ChooseUniformly(possibilities[:home_games])
        average_attendance ~ ChooseUniformly(possibilities[:average_attendance])
        total_attendance ~ ChooseUniformly(possibilities[:total_attendance])
        capacity_percentage ~ ChooseUniformly(possibilities[:capacity_percentage])
    end

    @class Game begin
        stadium ~ Stadium
        season ~ ChooseUniformly(possibilities[:season])
        date ~ ChooseUniformly(possibilities[:date])
        home_team ~ ChooseUniformly(possibilities[:home_team])
        away_team ~ ChooseUniformly(possibilities[:away_team])
        score ~ ChooseUniformly(possibilities[:score])
        competition ~ ChooseUniformly(possibilities[:competition])
    end

    @class Injury_accident begin
        game ~ Game
        player ~ ChooseUniformly(possibilities[:player])
        injury ~ ChooseUniformly(possibilities[:injury])
        number_of_matches ~ ChooseUniformly(possibilities[:number_of_matches])
        source ~ ChooseUniformly(possibilities[:source])
    end

    @class Obs begin
        injury_accident ~ Injury_accident
    end
end

query = @query GameInjuryModel.Obs [
    stadium_name injury_accident.game.stadium.name
    stadium_home_games injury_accident.game.stadium.home_games
    stadium_average_attendance injury_accident.game.stadium.average_attendance
    stadium_total_attendance injury_accident.game.stadium.total_attendance
    stadium_capacity_percentage injury_accident.game.stadium.capacity_percentage
    game_season injury_accident.game.season
    game_date injury_accident.game.date
    game_home_team injury_accident.game.home_team
    game_away_team injury_accident.game.away_team
    game_score injury_accident.game.score
    game_competition injury_accident.game.competition
    injury_accident_player injury_accident.player
    injury_accident_injury injury_accident.injury
    injury_accident_number_of_matches injury_accident.number_of_matches
    injury_accident_source injury_accident.source
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
