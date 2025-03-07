using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("country_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("country_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "country id"], Any[0, "country name"], Any[0, "capital"], Any[0, "official native language"], Any[1, "team id"], Any[1, "name"], Any[2, "season"], Any[2, "player"], Any[2, "position"], Any[2, "country"], Any[2, "team"], Any[2, "draft pick number"], Any[2, "draft class"], Any[2, "college"], Any[3, "player id"], Any[3, "player"], Any[3, "years played"], Any[3, "total wl"], Any[3, "singles wl"], Any[3, "doubles wl"], Any[3, "team"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "country id"], Any[0, "country name"], Any[0, "capital"], Any[0, "official native language"], Any[1, "team id"], Any[1, "name"], Any[2, "season"], Any[2, "player"], Any[2, "position"], Any[2, "country"], Any[2, "team"], Any[2, "draft pick number"], Any[2, "draft class"], Any[2, "college"], Any[3, "player id"], Any[3, "player"], Any[3, "years played"], Any[3, "total wl"], Any[3, "singles wl"], Any[3, "doubles wl"], Any[3, "team"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "country id"], Any[0, "country name"], Any[0, "capital"], Any[0, "official native language"], Any[1, "team id"], Any[1, "name"], Any[2, "season"], Any[2, "player"], Any[2, "position"], Any[2, "country"], Any[2, "team"], Any[2, "draft pick number"], Any[2, "draft class"], Any[2, "college"], Any[3, "player id"], Any[3, "player"], Any[3, "years played"], Any[3, "total wl"], Any[3, "singles wl"], Any[3, "doubles wl"], Any[3, "team"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "country id"], Any[0, "country name"], Any[0, "capital"], Any[0, "official native language"], Any[1, "team id"], Any[1, "name"], Any[2, "season"], Any[2, "player"], Any[2, "position"], Any[2, "country"], Any[2, "team"], Any[2, "draft pick number"], Any[2, "draft class"], Any[2, "college"], Any[3, "player id"], Any[3, "player"], Any[3, "years played"], Any[3, "total wl"], Any[3, "singles wl"], Any[3, "doubles wl"], Any[3, "team"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "country id"], Any[0, "country name"], Any[0, "capital"], Any[0, "official native language"], Any[1, "team id"], Any[1, "name"], Any[2, "season"], Any[2, "player"], Any[2, "position"], Any[2, "country"], Any[2, "team"], Any[2, "draft pick number"], Any[2, "draft class"], Any[2, "college"], Any[3, "player id"], Any[3, "player"], Any[3, "years played"], Any[3, "total wl"], Any[3, "singles wl"], Any[3, "doubles wl"], Any[3, "team"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 5], Any[10, 1], Any[21, 5]])
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







PClean.@model MatchSeasonModel begin
    @class Country begin
        country_name ~ ChooseUniformly(possibilities[:country_name])
        capital ~ ChooseUniformly(possibilities[:capital])
        official_native_language ~ ChooseUniformly(possibilities[:official_native_language])
    end

    @class Team begin
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Match_season begin
        season ~ ChooseUniformly(possibilities[:season])
        player ~ ChooseUniformly(possibilities[:player])
        position ~ ChooseUniformly(possibilities[:position])
        country ~ Country
        team ~ Team
        draft_pick_number ~ ChooseUniformly(possibilities[:draft_pick_number])
        draft_class ~ ChooseUniformly(possibilities[:draft_class])
        college ~ ChooseUniformly(possibilities[:college])
    end

    @class Player begin
        player ~ ChooseUniformly(possibilities[:player])
        years_played ~ ChooseUniformly(possibilities[:years_played])
        total_wl ~ ChooseUniformly(possibilities[:total_wl])
        singles_wl ~ ChooseUniformly(possibilities[:singles_wl])
        doubles_wl ~ ChooseUniformly(possibilities[:doubles_wl])
        team ~ Team
    end

    @class Obs begin
        match_season ~ Match_season
        player ~ Player
    end
end

query = @query MatchSeasonModel.Obs [
    country_id match_season.country.country_id
    country_name match_season.country.country_name
    country_capital match_season.country.capital
    country_official_native_language match_season.country.official_native_language
    team_id match_season.team.team_id
    team_name match_season.team.name
    match_season_season match_season.season
    match_season_player match_season.player
    match_season_position match_season.position
    match_season_draft_pick_number match_season.draft_pick_number
    match_season_draft_class match_season.draft_class
    match_season_college match_season.college
    player_id player.player_id
    player player.player
    player_years_played player.years_played
    player_total_wl player.total_wl
    player_singles_wl player.singles_wl
    player_doubles_wl player.doubles_wl
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
