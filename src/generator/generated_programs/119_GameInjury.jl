using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("stadium_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("stadium_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "stadium id"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "game id"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "stadium id"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "game id"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model GameInjuryModel begin
    @class Stadium begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        home_games ~ ChooseUniformly(possibilities[:home_games])
        average_attendance ~ ChooseUniformly(possibilities[:average_attendance])
        total_attendance ~ ChooseUniformly(possibilities[:total_attendance])
        capacity_percentage ~ ChooseUniformly(possibilities[:capacity_percentage])
    end

    @class Game begin
        stadium_id ~ Unmodeled()
        id ~ ChooseUniformly(possibilities[:id])
        season ~ ChooseUniformly(possibilities[:season])
        date ~ ChooseUniformly(possibilities[:date])
        home_team ~ ChooseUniformly(possibilities[:home_team])
        away_team ~ ChooseUniformly(possibilities[:away_team])
        score ~ ChooseUniformly(possibilities[:score])
        competition ~ ChooseUniformly(possibilities[:competition])
    end

    @class Injury_Accident begin
        game_id ~ Unmodeled()
        id ~ ChooseUniformly(possibilities[:id])
        player ~ ChooseUniformly(possibilities[:player])
        injury ~ ChooseUniformly(possibilities[:injury])
        number_of_matches ~ ChooseUniformly(possibilities[:number_of_matches])
        source ~ ChooseUniformly(possibilities[:source])
    end

    @class Obs begin
        stadium ~ Stadium
        game ~ Game
        injury_Accident ~ Injury_Accident
    end
end

query = @query GameInjuryModel.Obs [
    stadium_id stadium.id
    stadium_name stadium.name
    stadium_home_games stadium.home_games
    stadium_average_attendance stadium.average_attendance
    stadium_total_attendance stadium.total_attendance
    stadium_capacity_percentage stadium.capacity_percentage
    game_id game.id
    game_season game.season
    game_date game.date
    game_home_team game.home_team
    game_away_team game.away_team
    game_score game.score
    game_competition game.competition
    injury_accident_id injury_Accident.id
    injury_accident_player injury_Accident.player
    injury_accident_injury injury_Accident.injury
    injury_accident_number_of_matches injury_Accident.number_of_matches
    injury_accident_source injury_Accident.source
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
