using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("country_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("country_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["team", "country", "team"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "country id"], Any[0, "country name"], Any[0, "capital"], Any[0, "official native language"], Any[1, "team id"], Any[1, "name"], Any[2, "season"], Any[2, "player"], Any[2, "position"], Any[2, "draft pick number"], Any[2, "draft class"], Any[2, "college"], Any[3, "player id"], Any[3, "player"], Any[3, "years played"], Any[3, "total wl"], Any[3, "singles wl"], Any[3, "doubles wl"]]
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





PClean.@model MatchSeasonModel begin
    @class Country begin
        country_id ~ Unmodeled()
        country_name ~ ChooseUniformly(possibilities[:country_name])
        capital ~ ChooseUniformly(possibilities[:capital])
        official_native_language ~ ChooseUniformly(possibilities[:official_native_language])
    end

    @class Team begin
        team_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Obs begin
        country ~ Country
        team ~ Team
        season ~ ChooseUniformly(possibilities[:season])
        player ~ ChooseUniformly(possibilities[:player])
        position ~ ChooseUniformly(possibilities[:position])
        draft_pick_number ~ ChooseUniformly(possibilities[:draft_pick_number])
        draft_class ~ ChooseUniformly(possibilities[:draft_class])
        college ~ ChooseUniformly(possibilities[:college])
        player_id ~ Unmodeled()
        player ~ ChooseUniformly(possibilities[:player])
        years_played ~ ChooseUniformly(possibilities[:years_played])
        total_wl ~ ChooseUniformly(possibilities[:total_wl])
        singles_wl ~ ChooseUniformly(possibilities[:singles_wl])
        doubles_wl ~ ChooseUniformly(possibilities[:doubles_wl])
    end
end

query = @query MatchSeasonModel.Obs [
    country_id country.country_id
    country_name country.country_name
    country_capital country.capital
    country_official_native_language country.official_native_language
    team_id team.team_id
    team_name team.name
    match_season_season season
    match_season_player player
    match_season_position position
    match_season_draft_pick_number draft_pick_number
    match_season_draft_class draft_class
    match_season_college college
    player_id player_id
    player player
    player_years_played years_played
    player_total_wl total_wl
    player_singles_wl singles_wl
    player_doubles_wl doubles_wl
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
