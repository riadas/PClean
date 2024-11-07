using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("club_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("club_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "club id"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "club id"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "club id"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "competition id"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "competition id"], Any[4, "club id 1"], Any[4, "club id 2"], Any[4, "score"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "club id"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "club id"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "club id"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "competition id"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "competition id"], Any[4, "club id 1"], Any[4, "club id 2"], Any[4, "score"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model SportsCompetitionModel begin
    @class Club begin
        club_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        region ~ ChooseUniformly(possibilities[:region])
        start_year ~ ChooseUniformly(possibilities[:start_year])
    end

    @class Club_Rank begin
        rank ~ ChooseUniformly(possibilities[:rank])
        club_id ~ ChooseUniformly(possibilities[:club_id])
        gold ~ ChooseUniformly(possibilities[:gold])
        silver ~ ChooseUniformly(possibilities[:silver])
        bronze ~ ChooseUniformly(possibilities[:bronze])
        total ~ ChooseUniformly(possibilities[:total])
    end

    @class Player begin
        player_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        position ~ ChooseUniformly(possibilities[:position])
        club_id ~ ChooseUniformly(possibilities[:club_id])
        apps ~ ChooseUniformly(possibilities[:apps])
        tries ~ ChooseUniformly(possibilities[:tries])
        goals ~ ChooseUniformly(possibilities[:goals])
        points ~ ChooseUniformly(possibilities[:points])
    end

    @class Competition begin
        competition_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        competition_type ~ ChooseUniformly(possibilities[:competition_type])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Competition_Result begin
        competition_id ~ Unmodeled()
        club_id_1 ~ ChooseUniformly(possibilities[:club_id_1])
        club_id_2 ~ ChooseUniformly(possibilities[:club_id_2])
        score ~ ChooseUniformly(possibilities[:score])
    end

    @class Obs begin
        club ~ Club
        club_Rank ~ Club_Rank
        player ~ Player
        competition ~ Competition
        competition_Result ~ Competition_Result
    end
end

query = @query SportsCompetitionModel.Obs [
    club_id club.club_id
    club_name club.name
    club_region club.region
    club_start_year club.start_year
    club_rank_rank club_Rank.rank
    club_rank_gold club_Rank.gold
    club_rank_silver club_Rank.silver
    club_rank_bronze club_Rank.bronze
    club_rank_total club_Rank.total
    player_id player.player_id
    player_name player.name
    player_position player.position
    player_apps player.apps
    player_tries player.tries
    player_goals player.goals
    player_points player.points
    competition_id competition.competition_id
    competition_year competition.year
    competition_type competition.competition_type
    competition_country competition.country
    competition_result_score competition_Result.score
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
