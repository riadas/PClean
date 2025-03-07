using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("club_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("club_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "club id"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "club id"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "club id"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "competition id"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "competition id"], Any[4, "club id 1"], Any[4, "club id 2"], Any[4, "score"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "club id"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "club id"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "club id"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "competition id"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "competition id"], Any[4, "club id 1"], Any[4, "club id 2"], Any[4, "score"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "club id"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "club id"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "club id"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "competition id"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "competition id"], Any[4, "club id 1"], Any[4, "club id 2"], Any[4, "score"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "club id"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "club id"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "club id"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "competition id"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "competition id"], Any[4, "club id 1"], Any[4, "club id 2"], Any[4, "score"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "club id"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "club id"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "club id"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "competition id"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "competition id"], Any[4, "club id 1"], Any[4, "club id 2"], Any[4, "score"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[6, 1], Any[14, 1], Any[23, 19], Any[25, 1], Any[24, 1]])
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







PClean.@model SportsCompetitionModel begin
    @class Club begin
        name ~ ChooseUniformly(possibilities[:name])
        region ~ ChooseUniformly(possibilities[:region])
        start_year ~ ChooseUniformly(possibilities[:start_year])
    end

    @class Club_rank begin
        rank ~ ChooseUniformly(possibilities[:rank])
        club ~ Club
        gold ~ ChooseUniformly(possibilities[:gold])
        silver ~ ChooseUniformly(possibilities[:silver])
        bronze ~ ChooseUniformly(possibilities[:bronze])
        total ~ ChooseUniformly(possibilities[:total])
    end

    @class Player begin
        name ~ ChooseUniformly(possibilities[:name])
        position ~ ChooseUniformly(possibilities[:position])
        club ~ Club
        apps ~ ChooseUniformly(possibilities[:apps])
        tries ~ ChooseUniformly(possibilities[:tries])
        goals ~ ChooseUniformly(possibilities[:goals])
        points ~ ChooseUniformly(possibilities[:points])
    end

    @class Competition begin
        year ~ ChooseUniformly(possibilities[:year])
        competition_type ~ ChooseUniformly(possibilities[:competition_type])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Competition_result begin
        club ~ Club
        score ~ ChooseUniformly(possibilities[:score])
    end

    @class Obs begin
        club_rank ~ Club_rank
        player ~ Player
        competition_result ~ Competition_result
    end
end

query = @query SportsCompetitionModel.Obs [
    club_id club_rank.club.club_id
    club_name club_rank.club.name
    club_region club_rank.club.region
    club_start_year club_rank.club.start_year
    club_rank_rank club_rank.rank
    club_rank_gold club_rank.gold
    club_rank_silver club_rank.silver
    club_rank_bronze club_rank.bronze
    club_rank_total club_rank.total
    player_id player.player_id
    player_name player.name
    player_position player.position
    player_apps player.apps
    player_tries player.tries
    player_goals player.goals
    player_points player.points
    competition_id competition_result.competition.competition_id
    competition_year competition_result.competition.year
    competition_type competition_result.competition.competition_type
    competition_country competition_result.competition.country
    competition_result_score competition_result.score
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
