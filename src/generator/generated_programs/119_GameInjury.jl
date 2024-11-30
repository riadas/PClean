using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("stadium_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("stadium_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["stadium id", "game id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "home games"], Any[0, "average attendance"], Any[0, "total attendance"], Any[0, "capacity percentage"], Any[1, "id"], Any[1, "season"], Any[1, "date"], Any[1, "home team"], Any[1, "away team"], Any[1, "score"], Any[1, "competition"], Any[2, "id"], Any[2, "player"], Any[2, "injury"], Any[2, "number of matches"], Any[2, "source"]]
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





PClean.@model GameInjuryModel begin
    @class Stadium begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        home_games ~ ChooseUniformly(possibilities[:home_games])
        average_attendance ~ ChooseUniformly(possibilities[:average_attendance])
        total_attendance ~ ChooseUniformly(possibilities[:total_attendance])
        capacity_percentage ~ ChooseUniformly(possibilities[:capacity_percentage])
    end

    @class Obs begin
        stadium ~ Stadium
        id ~ ChooseUniformly(possibilities[:id])
        season ~ ChooseUniformly(possibilities[:season])
        date ~ ChooseUniformly(possibilities[:date])
        home_team ~ ChooseUniformly(possibilities[:home_team])
        away_team ~ ChooseUniformly(possibilities[:away_team])
        score ~ ChooseUniformly(possibilities[:score])
        competition ~ ChooseUniformly(possibilities[:competition])
        id ~ ChooseUniformly(possibilities[:id])
        player ~ ChooseUniformly(possibilities[:player])
        injury ~ ChooseUniformly(possibilities[:injury])
        number_of_matches ~ ChooseUniformly(possibilities[:number_of_matches])
        source ~ ChooseUniformly(possibilities[:source])
    end
end

query = @query GameInjuryModel.Obs [
    stadium_id stadium.id
    stadium_name stadium.name
    stadium_home_games stadium.home_games
    stadium_average_attendance stadium.average_attendance
    stadium_total_attendance stadium.total_attendance
    stadium_capacity_percentage stadium.capacity_percentage
    game_id id
    game_season season
    game_date date
    game_home_team home_team
    game_away_team away_team
    game_score score
    game_competition competition
    injury_accident_id id
    injury_accident_player player
    injury_accident_injury injury
    injury_accident_number_of_matches number_of_matches
    injury_accident_source source
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
