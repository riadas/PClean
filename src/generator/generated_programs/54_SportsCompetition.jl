using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("club_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("club_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["club id", "club id", "competition id", "club id 2", "club id 1"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "region"], Any[0, "start year"], Any[1, "rank"], Any[1, "gold"], Any[1, "silver"], Any[1, "bronze"], Any[1, "total"], Any[2, "player id"], Any[2, "name"], Any[2, "position"], Any[2, "apps"], Any[2, "tries"], Any[2, "goals"], Any[2, "points"], Any[3, "year"], Any[3, "competition type"], Any[3, "country"], Any[4, "score"]]
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





PClean.@model SportsCompetitionModel begin
    @class Club begin
        club_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        region ~ ChooseUniformly(possibilities[:region])
        start_year ~ ChooseUniformly(possibilities[:start_year])
    end

    @class Competition begin
        competition_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        competition_type ~ ChooseUniformly(possibilities[:competition_type])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Obs begin
        club ~ Club
        competition ~ Competition
        rank ~ ChooseUniformly(possibilities[:rank])
        gold ~ ChooseUniformly(possibilities[:gold])
        silver ~ ChooseUniformly(possibilities[:silver])
        bronze ~ ChooseUniformly(possibilities[:bronze])
        total ~ ChooseUniformly(possibilities[:total])
        player_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        position ~ ChooseUniformly(possibilities[:position])
        apps ~ ChooseUniformly(possibilities[:apps])
        tries ~ ChooseUniformly(possibilities[:tries])
        goals ~ ChooseUniformly(possibilities[:goals])
        points ~ ChooseUniformly(possibilities[:points])
        score ~ ChooseUniformly(possibilities[:score])
    end
end

query = @query SportsCompetitionModel.Obs [
    club_id club.club_id
    club_name club.name
    club_region club.region
    club_start_year club.start_year
    club_rank_rank rank
    club_rank_gold gold
    club_rank_silver silver
    club_rank_bronze bronze
    club_rank_total total
    player_id player_id
    player_name name
    player_position position
    player_apps apps
    player_tries tries
    player_goals goals
    player_points points
    competition_id competition.competition_id
    competition_year competition.year
    competition_type competition.competition_type
    competition_country competition.country
    competition_result_score score
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
