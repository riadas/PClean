using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("player attributes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("player attributes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "player fifa api id"], Any[0, "player api id"], Any[0, "date"], Any[0, "overall rating"], Any[0, "potential"], Any[0, "preferred foot"], Any[0, "attacking work rate"], Any[0, "defensive work rate"], Any[0, "crossing"], Any[0, "finishing"], Any[0, "heading accuracy"], Any[0, "short passing"], Any[0, "volleys"], Any[0, "dribbling"], Any[0, "curve"], Any[0, "free kick accuracy"], Any[0, "long passing"], Any[0, "ball control"], Any[0, "acceleration"], Any[0, "sprint speed"], Any[0, "agility"], Any[0, "reactions"], Any[0, "balance"], Any[0, "shot power"], Any[0, "jumping"], Any[0, "stamina"], Any[0, "strength"], Any[0, "long shots"], Any[0, "aggression"], Any[0, "interceptions"], Any[0, "positioning"], Any[0, "vision"], Any[0, "penalties"], Any[0, "marking"], Any[0, "standing tackle"], Any[0, "sliding tackle"], Any[0, "gk diving"], Any[0, "gk handling"], Any[0, "gk kicking"], Any[0, "gk positioning"], Any[0, "gk reflexes"], Any[1, "name"], Any[1, "seq"], Any[2, "id"], Any[2, "player api id"], Any[2, "player name"], Any[2, "player fifa api id"], Any[2, "birthday"], Any[2, "height"], Any[2, "weight"], Any[3, "id"], Any[3, "country id"], Any[3, "name"], Any[4, "id"], Any[4, "name"], Any[5, "id"], Any[5, "team api id"], Any[5, "team fifa api id"], Any[5, "team long name"], Any[5, "team short name"], Any[6, "id"], Any[6, "team fifa api id"], Any[6, "team api id"], Any[6, "date"], Any[6, "buildup play speed"], Any[6, "buildup play speed class"], Any[6, "buildup play dribbling"], Any[6, "buildup play dribbling class"], Any[6, "buildup play passing"], Any[6, "buildup play passing class"], Any[6, "buildup play positioning class"], Any[6, "chance creation passing"], Any[6, "chance creation passing class"], Any[6, "chance creation crossing"], Any[6, "chance creation crossing class"], Any[6, "chance creation shooting"], Any[6, "chance creation shooting class"], Any[6, "chance creation positioning class"], Any[6, "defence pressure"], Any[6, "defence pressure class"], Any[6, "defence aggression"], Any[6, "defence aggression class"], Any[6, "defence team width"], Any[6, "defence team width class"], Any[6, "defence defender line class"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "player fifa api id"], Any[0, "player api id"], Any[0, "date"], Any[0, "overall rating"], Any[0, "potential"], Any[0, "preferred foot"], Any[0, "attacking work rate"], Any[0, "defensive work rate"], Any[0, "crossing"], Any[0, "finishing"], Any[0, "heading accuracy"], Any[0, "short passing"], Any[0, "volleys"], Any[0, "dribbling"], Any[0, "curve"], Any[0, "free kick accuracy"], Any[0, "long passing"], Any[0, "ball control"], Any[0, "acceleration"], Any[0, "sprint speed"], Any[0, "agility"], Any[0, "reactions"], Any[0, "balance"], Any[0, "shot power"], Any[0, "jumping"], Any[0, "stamina"], Any[0, "strength"], Any[0, "long shots"], Any[0, "aggression"], Any[0, "interceptions"], Any[0, "positioning"], Any[0, "vision"], Any[0, "penalties"], Any[0, "marking"], Any[0, "standing tackle"], Any[0, "sliding tackle"], Any[0, "gk diving"], Any[0, "gk handling"], Any[0, "gk kicking"], Any[0, "gk positioning"], Any[0, "gk reflexes"], Any[1, "name"], Any[1, "seq"], Any[2, "id"], Any[2, "player api id"], Any[2, "player name"], Any[2, "player fifa api id"], Any[2, "birthday"], Any[2, "height"], Any[2, "weight"], Any[3, "id"], Any[3, "country id"], Any[3, "name"], Any[4, "id"], Any[4, "name"], Any[5, "id"], Any[5, "team api id"], Any[5, "team fifa api id"], Any[5, "team long name"], Any[5, "team short name"], Any[6, "id"], Any[6, "team fifa api id"], Any[6, "team api id"], Any[6, "date"], Any[6, "buildup play speed"], Any[6, "buildup play speed class"], Any[6, "buildup play dribbling"], Any[6, "buildup play dribbling class"], Any[6, "buildup play passing"], Any[6, "buildup play passing class"], Any[6, "buildup play positioning class"], Any[6, "chance creation passing"], Any[6, "chance creation passing class"], Any[6, "chance creation crossing"], Any[6, "chance creation crossing class"], Any[6, "chance creation shooting"], Any[6, "chance creation shooting class"], Any[6, "chance creation positioning class"], Any[6, "defence pressure"], Any[6, "defence pressure class"], Any[6, "defence aggression"], Any[6, "defence aggression class"], Any[6, "defence team width"], Any[6, "defence team width class"], Any[6, "defence defender line class"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["player api id", "player fifa api id", "country id", "team api id", "team fifa api id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "date"], Any[0, "overall rating"], Any[0, "potential"], Any[0, "preferred foot"], Any[0, "attacking work rate"], Any[0, "defensive work rate"], Any[0, "crossing"], Any[0, "finishing"], Any[0, "heading accuracy"], Any[0, "short passing"], Any[0, "volleys"], Any[0, "dribbling"], Any[0, "curve"], Any[0, "free kick accuracy"], Any[0, "long passing"], Any[0, "ball control"], Any[0, "acceleration"], Any[0, "sprint speed"], Any[0, "agility"], Any[0, "reactions"], Any[0, "balance"], Any[0, "shot power"], Any[0, "jumping"], Any[0, "stamina"], Any[0, "strength"], Any[0, "long shots"], Any[0, "aggression"], Any[0, "interceptions"], Any[0, "positioning"], Any[0, "vision"], Any[0, "penalties"], Any[0, "marking"], Any[0, "standing tackle"], Any[0, "sliding tackle"], Any[0, "gk diving"], Any[0, "gk handling"], Any[0, "gk kicking"], Any[0, "gk positioning"], Any[0, "gk reflexes"], Any[1, "name"], Any[1, "seq"], Any[2, "id"], Any[2, "player name"], Any[2, "birthday"], Any[2, "height"], Any[2, "weight"], Any[3, "id"], Any[3, "name"], Any[4, "id"], Any[4, "name"], Any[5, "id"], Any[5, "team long name"], Any[5, "team short name"], Any[6, "id"], Any[6, "date"], Any[6, "buildup play speed"], Any[6, "buildup play speed class"], Any[6, "buildup play dribbling"], Any[6, "buildup play dribbling class"], Any[6, "buildup play passing"], Any[6, "buildup play passing class"], Any[6, "buildup play positioning class"], Any[6, "chance creation passing"], Any[6, "chance creation passing class"], Any[6, "chance creation crossing"], Any[6, "chance creation crossing class"], Any[6, "chance creation shooting"], Any[6, "chance creation shooting class"], Any[6, "chance creation positioning class"], Any[6, "defence pressure"], Any[6, "defence pressure class"], Any[6, "defence aggression"], Any[6, "defence aggression class"], Any[6, "defence team width"], Any[6, "defence team width class"], Any[6, "defence defender line class"]]
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





PClean.@model Soccer1Model begin
    @class Sqlite_Sequence begin
        name ~ ChooseUniformly(possibilities[:name])
        seq ~ ChooseUniformly(possibilities[:seq])
    end

    @class Player begin
        id ~ Unmodeled()
        player_api_id ~ ChooseUniformly(possibilities[:player_api_id])
        player_name ~ ChooseUniformly(possibilities[:player_name])
        player_fifa_api_id ~ ChooseUniformly(possibilities[:player_fifa_api_id])
        birthday ~ ChooseUniformly(possibilities[:birthday])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
    end

    @class Country begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Team begin
        id ~ Unmodeled()
        team_api_id ~ ChooseUniformly(possibilities[:team_api_id])
        team_fifa_api_id ~ ChooseUniformly(possibilities[:team_fifa_api_id])
        team_long_name ~ ChooseUniformly(possibilities[:team_long_name])
        team_short_name ~ ChooseUniformly(possibilities[:team_short_name])
    end

    @class Obs begin
        sqlite_Sequence ~ Sqlite_Sequence
        player ~ Player
        country ~ Country
        team ~ Team
        id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        overall_rating ~ ChooseUniformly(possibilities[:overall_rating])
        potential ~ ChooseUniformly(possibilities[:potential])
        preferred_foot ~ ChooseUniformly(possibilities[:preferred_foot])
        attacking_work_rate ~ ChooseUniformly(possibilities[:attacking_work_rate])
        defensive_work_rate ~ ChooseUniformly(possibilities[:defensive_work_rate])
        crossing ~ ChooseUniformly(possibilities[:crossing])
        finishing ~ ChooseUniformly(possibilities[:finishing])
        heading_accuracy ~ ChooseUniformly(possibilities[:heading_accuracy])
        short_passing ~ ChooseUniformly(possibilities[:short_passing])
        volleys ~ ChooseUniformly(possibilities[:volleys])
        dribbling ~ ChooseUniformly(possibilities[:dribbling])
        curve ~ ChooseUniformly(possibilities[:curve])
        free_kick_accuracy ~ ChooseUniformly(possibilities[:free_kick_accuracy])
        long_passing ~ ChooseUniformly(possibilities[:long_passing])
        ball_control ~ ChooseUniformly(possibilities[:ball_control])
        acceleration ~ ChooseUniformly(possibilities[:acceleration])
        sprint_speed ~ ChooseUniformly(possibilities[:sprint_speed])
        agility ~ ChooseUniformly(possibilities[:agility])
        reactions ~ ChooseUniformly(possibilities[:reactions])
        balance ~ ChooseUniformly(possibilities[:balance])
        shot_power ~ ChooseUniformly(possibilities[:shot_power])
        jumping ~ ChooseUniformly(possibilities[:jumping])
        stamina ~ ChooseUniformly(possibilities[:stamina])
        strength ~ ChooseUniformly(possibilities[:strength])
        long_shots ~ ChooseUniformly(possibilities[:long_shots])
        aggression ~ ChooseUniformly(possibilities[:aggression])
        interceptions ~ ChooseUniformly(possibilities[:interceptions])
        positioning ~ ChooseUniformly(possibilities[:positioning])
        vision ~ ChooseUniformly(possibilities[:vision])
        penalties ~ ChooseUniformly(possibilities[:penalties])
        marking ~ ChooseUniformly(possibilities[:marking])
        standing_tackle ~ ChooseUniformly(possibilities[:standing_tackle])
        sliding_tackle ~ ChooseUniformly(possibilities[:sliding_tackle])
        gk_diving ~ ChooseUniformly(possibilities[:gk_diving])
        gk_handling ~ ChooseUniformly(possibilities[:gk_handling])
        gk_kicking ~ ChooseUniformly(possibilities[:gk_kicking])
        gk_positioning ~ ChooseUniformly(possibilities[:gk_positioning])
        gk_reflexes ~ ChooseUniformly(possibilities[:gk_reflexes])
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        buildup_play_speed ~ ChooseUniformly(possibilities[:buildup_play_speed])
        buildup_play_speed_class ~ ChooseUniformly(possibilities[:buildup_play_speed_class])
        buildup_play_dribbling ~ ChooseUniformly(possibilities[:buildup_play_dribbling])
        buildup_play_dribbling_class ~ ChooseUniformly(possibilities[:buildup_play_dribbling_class])
        buildup_play_passing ~ ChooseUniformly(possibilities[:buildup_play_passing])
        buildup_play_passing_class ~ ChooseUniformly(possibilities[:buildup_play_passing_class])
        buildup_play_positioning_class ~ ChooseUniformly(possibilities[:buildup_play_positioning_class])
        chance_creation_passing ~ ChooseUniformly(possibilities[:chance_creation_passing])
        chance_creation_passing_class ~ ChooseUniformly(possibilities[:chance_creation_passing_class])
        chance_creation_crossing ~ ChooseUniformly(possibilities[:chance_creation_crossing])
        chance_creation_crossing_class ~ ChooseUniformly(possibilities[:chance_creation_crossing_class])
        chance_creation_shooting ~ ChooseUniformly(possibilities[:chance_creation_shooting])
        chance_creation_shooting_class ~ ChooseUniformly(possibilities[:chance_creation_shooting_class])
        chance_creation_positioning_class ~ ChooseUniformly(possibilities[:chance_creation_positioning_class])
        defence_pressure ~ ChooseUniformly(possibilities[:defence_pressure])
        defence_pressure_class ~ ChooseUniformly(possibilities[:defence_pressure_class])
        defence_aggression ~ ChooseUniformly(possibilities[:defence_aggression])
        defence_aggression_class ~ ChooseUniformly(possibilities[:defence_aggression_class])
        defence_team_width ~ ChooseUniformly(possibilities[:defence_team_width])
        defence_team_width_class ~ ChooseUniformly(possibilities[:defence_team_width_class])
        defence_defender_line_class ~ ChooseUniformly(possibilities[:defence_defender_line_class])
    end
end

query = @query Soccer1Model.Obs [
    player_attributes_id id
    player_attributes_date date
    player_attributes_overall_rating overall_rating
    player_attributes_potential potential
    player_attributes_preferred_foot preferred_foot
    player_attributes_attacking_work_rate attacking_work_rate
    player_attributes_defensive_work_rate defensive_work_rate
    player_attributes_crossing crossing
    player_attributes_finishing finishing
    player_attributes_heading_accuracy heading_accuracy
    player_attributes_short_passing short_passing
    player_attributes_volleys volleys
    player_attributes_dribbling dribbling
    player_attributes_curve curve
    player_attributes_free_kick_accuracy free_kick_accuracy
    player_attributes_long_passing long_passing
    player_attributes_ball_control ball_control
    player_attributes_acceleration acceleration
    player_attributes_sprint_speed sprint_speed
    player_attributes_agility agility
    player_attributes_reactions reactions
    player_attributes_balance balance
    player_attributes_shot_power shot_power
    player_attributes_jumping jumping
    player_attributes_stamina stamina
    player_attributes_strength strength
    player_attributes_long_shots long_shots
    player_attributes_aggression aggression
    player_attributes_interceptions interceptions
    player_attributes_positioning positioning
    player_attributes_vision vision
    player_attributes_penalties penalties
    player_attributes_marking marking
    player_attributes_standing_tackle standing_tackle
    player_attributes_sliding_tackle sliding_tackle
    player_attributes_gk_diving gk_diving
    player_attributes_gk_handling gk_handling
    player_attributes_gk_kicking gk_kicking
    player_attributes_gk_positioning gk_positioning
    player_attributes_gk_reflexes gk_reflexes
    sqlite_sequence_name sqlite_Sequence.name
    sqlite_sequence_seq sqlite_Sequence.seq
    player_id player.id
    player_api_id player.player_api_id
    player_name player.player_name
    player_fifa_api_id player.player_fifa_api_id
    player_birthday player.birthday
    player_height player.height
    player_weight player.weight
    league_id id
    league_name name
    country_id country.id
    country_name country.name
    team_id team.id
    team_api_id team.team_api_id
    team_fifa_api_id team.team_fifa_api_id
    team_long_name team.team_long_name
    team_short_name team.team_short_name
    team_attributes_id id
    team_attributes_date date
    team_attributes_buildup_play_speed buildup_play_speed
    team_attributes_buildup_play_speed_class buildup_play_speed_class
    team_attributes_buildup_play_dribbling buildup_play_dribbling
    team_attributes_buildup_play_dribbling_class buildup_play_dribbling_class
    team_attributes_buildup_play_passing buildup_play_passing
    team_attributes_buildup_play_passing_class buildup_play_passing_class
    team_attributes_buildup_play_positioning_class buildup_play_positioning_class
    team_attributes_chance_creation_passing chance_creation_passing
    team_attributes_chance_creation_passing_class chance_creation_passing_class
    team_attributes_chance_creation_crossing chance_creation_crossing
    team_attributes_chance_creation_crossing_class chance_creation_crossing_class
    team_attributes_chance_creation_shooting chance_creation_shooting
    team_attributes_chance_creation_shooting_class chance_creation_shooting_class
    team_attributes_chance_creation_positioning_class chance_creation_positioning_class
    team_attributes_defence_pressure defence_pressure
    team_attributes_defence_pressure_class defence_pressure_class
    team_attributes_defence_aggression defence_aggression
    team_attributes_defence_aggression_class defence_aggression_class
    team_attributes_defence_team_width defence_team_width
    team_attributes_defence_team_width_class defence_team_width_class
    team_attributes_defence_defender_line_class defence_defender_line_class
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
