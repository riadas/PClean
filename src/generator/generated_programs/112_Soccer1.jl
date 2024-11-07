using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("player attributes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("player attributes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "player fifa api id"], Any[0, "player api id"], Any[0, "date"], Any[0, "overall rating"], Any[0, "potential"], Any[0, "preferred foot"], Any[0, "attacking work rate"], Any[0, "defensive work rate"], Any[0, "crossing"], Any[0, "finishing"], Any[0, "heading accuracy"], Any[0, "short passing"], Any[0, "volleys"], Any[0, "dribbling"], Any[0, "curve"], Any[0, "free kick accuracy"], Any[0, "long passing"], Any[0, "ball control"], Any[0, "acceleration"], Any[0, "sprint speed"], Any[0, "agility"], Any[0, "reactions"], Any[0, "balance"], Any[0, "shot power"], Any[0, "jumping"], Any[0, "stamina"], Any[0, "strength"], Any[0, "long shots"], Any[0, "aggression"], Any[0, "interceptions"], Any[0, "positioning"], Any[0, "vision"], Any[0, "penalties"], Any[0, "marking"], Any[0, "standing tackle"], Any[0, "sliding tackle"], Any[0, "gk diving"], Any[0, "gk handling"], Any[0, "gk kicking"], Any[0, "gk positioning"], Any[0, "gk reflexes"], Any[1, "name"], Any[1, "seq"], Any[2, "id"], Any[2, "player api id"], Any[2, "player name"], Any[2, "player fifa api id"], Any[2, "birthday"], Any[2, "height"], Any[2, "weight"], Any[3, "id"], Any[3, "country id"], Any[3, "name"], Any[4, "id"], Any[4, "name"], Any[5, "id"], Any[5, "team api id"], Any[5, "team fifa api id"], Any[5, "team long name"], Any[5, "team short name"], Any[6, "id"], Any[6, "team fifa api id"], Any[6, "team api id"], Any[6, "date"], Any[6, "buildup play speed"], Any[6, "buildup play speed class"], Any[6, "buildup play dribbling"], Any[6, "buildup play dribbling class"], Any[6, "buildup play passing"], Any[6, "buildup play passing class"], Any[6, "buildup play positioning class"], Any[6, "chance creation passing"], Any[6, "chance creation passing class"], Any[6, "chance creation crossing"], Any[6, "chance creation crossing class"], Any[6, "chance creation shooting"], Any[6, "chance creation shooting class"], Any[6, "chance creation positioning class"], Any[6, "defence pressure"], Any[6, "defence pressure class"], Any[6, "defence aggression"], Any[6, "defence aggression class"], Any[6, "defence team width"], Any[6, "defence team width class"], Any[6, "defence defender line class"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "player fifa api id"], Any[0, "player api id"], Any[0, "date"], Any[0, "overall rating"], Any[0, "potential"], Any[0, "preferred foot"], Any[0, "attacking work rate"], Any[0, "defensive work rate"], Any[0, "crossing"], Any[0, "finishing"], Any[0, "heading accuracy"], Any[0, "short passing"], Any[0, "volleys"], Any[0, "dribbling"], Any[0, "curve"], Any[0, "free kick accuracy"], Any[0, "long passing"], Any[0, "ball control"], Any[0, "acceleration"], Any[0, "sprint speed"], Any[0, "agility"], Any[0, "reactions"], Any[0, "balance"], Any[0, "shot power"], Any[0, "jumping"], Any[0, "stamina"], Any[0, "strength"], Any[0, "long shots"], Any[0, "aggression"], Any[0, "interceptions"], Any[0, "positioning"], Any[0, "vision"], Any[0, "penalties"], Any[0, "marking"], Any[0, "standing tackle"], Any[0, "sliding tackle"], Any[0, "gk diving"], Any[0, "gk handling"], Any[0, "gk kicking"], Any[0, "gk positioning"], Any[0, "gk reflexes"], Any[1, "name"], Any[1, "seq"], Any[2, "id"], Any[2, "player api id"], Any[2, "player name"], Any[2, "player fifa api id"], Any[2, "birthday"], Any[2, "height"], Any[2, "weight"], Any[3, "id"], Any[3, "country id"], Any[3, "name"], Any[4, "id"], Any[4, "name"], Any[5, "id"], Any[5, "team api id"], Any[5, "team fifa api id"], Any[5, "team long name"], Any[5, "team short name"], Any[6, "id"], Any[6, "team fifa api id"], Any[6, "team api id"], Any[6, "date"], Any[6, "buildup play speed"], Any[6, "buildup play speed class"], Any[6, "buildup play dribbling"], Any[6, "buildup play dribbling class"], Any[6, "buildup play passing"], Any[6, "buildup play passing class"], Any[6, "buildup play positioning class"], Any[6, "chance creation passing"], Any[6, "chance creation passing class"], Any[6, "chance creation crossing"], Any[6, "chance creation crossing class"], Any[6, "chance creation shooting"], Any[6, "chance creation shooting class"], Any[6, "chance creation positioning class"], Any[6, "defence pressure"], Any[6, "defence pressure class"], Any[6, "defence aggression"], Any[6, "defence aggression class"], Any[6, "defence team width"], Any[6, "defence team width class"], Any[6, "defence defender line class"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Soccer1Model begin
    @class Player_Attributes begin
        id ~ Unmodeled()
        player_fifa_api_id ~ ChooseUniformly(possibilities[:player_fifa_api_id])
        player_api_id ~ ChooseUniformly(possibilities[:player_api_id])
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
    end

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

    @class League begin
        id ~ Unmodeled()
        country_id ~ ChooseUniformly(possibilities[:country_id])
        name ~ ChooseUniformly(possibilities[:name])
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

    @class Team_Attributes begin
        id ~ Unmodeled()
        team_fifa_api_id ~ ChooseUniformly(possibilities[:team_fifa_api_id])
        team_api_id ~ ChooseUniformly(possibilities[:team_api_id])
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

    @class Obs begin
        player_Attributes ~ Player_Attributes
        sqlite_Sequence ~ Sqlite_Sequence
        player ~ Player
        league ~ League
        country ~ Country
        team ~ Team
        team_Attributes ~ Team_Attributes
    end
end

query = @query Soccer1Model.Obs [
    player_attributes_id player_Attributes.id
    player_attributes_date player_Attributes.date
    player_attributes_overall_rating player_Attributes.overall_rating
    player_attributes_potential player_Attributes.potential
    player_attributes_preferred_foot player_Attributes.preferred_foot
    player_attributes_attacking_work_rate player_Attributes.attacking_work_rate
    player_attributes_defensive_work_rate player_Attributes.defensive_work_rate
    player_attributes_crossing player_Attributes.crossing
    player_attributes_finishing player_Attributes.finishing
    player_attributes_heading_accuracy player_Attributes.heading_accuracy
    player_attributes_short_passing player_Attributes.short_passing
    player_attributes_volleys player_Attributes.volleys
    player_attributes_dribbling player_Attributes.dribbling
    player_attributes_curve player_Attributes.curve
    player_attributes_free_kick_accuracy player_Attributes.free_kick_accuracy
    player_attributes_long_passing player_Attributes.long_passing
    player_attributes_ball_control player_Attributes.ball_control
    player_attributes_acceleration player_Attributes.acceleration
    player_attributes_sprint_speed player_Attributes.sprint_speed
    player_attributes_agility player_Attributes.agility
    player_attributes_reactions player_Attributes.reactions
    player_attributes_balance player_Attributes.balance
    player_attributes_shot_power player_Attributes.shot_power
    player_attributes_jumping player_Attributes.jumping
    player_attributes_stamina player_Attributes.stamina
    player_attributes_strength player_Attributes.strength
    player_attributes_long_shots player_Attributes.long_shots
    player_attributes_aggression player_Attributes.aggression
    player_attributes_interceptions player_Attributes.interceptions
    player_attributes_positioning player_Attributes.positioning
    player_attributes_vision player_Attributes.vision
    player_attributes_penalties player_Attributes.penalties
    player_attributes_marking player_Attributes.marking
    player_attributes_standing_tackle player_Attributes.standing_tackle
    player_attributes_sliding_tackle player_Attributes.sliding_tackle
    player_attributes_gk_diving player_Attributes.gk_diving
    player_attributes_gk_handling player_Attributes.gk_handling
    player_attributes_gk_kicking player_Attributes.gk_kicking
    player_attributes_gk_positioning player_Attributes.gk_positioning
    player_attributes_gk_reflexes player_Attributes.gk_reflexes
    sqlite_sequence_name sqlite_Sequence.name
    sqlite_sequence_seq sqlite_Sequence.seq
    player_id player.id
    player_api_id player.player_api_id
    player_name player.player_name
    player_fifa_api_id player.player_fifa_api_id
    player_birthday player.birthday
    player_height player.height
    player_weight player.weight
    league_id league.id
    league_name league.name
    country_id country.id
    country_name country.name
    team_id team.id
    team_api_id team.team_api_id
    team_fifa_api_id team.team_fifa_api_id
    team_long_name team.team_long_name
    team_short_name team.team_short_name
    team_attributes_id team_Attributes.id
    team_attributes_date team_Attributes.date
    team_attributes_buildup_play_speed team_Attributes.buildup_play_speed
    team_attributes_buildup_play_speed_class team_Attributes.buildup_play_speed_class
    team_attributes_buildup_play_dribbling team_Attributes.buildup_play_dribbling
    team_attributes_buildup_play_dribbling_class team_Attributes.buildup_play_dribbling_class
    team_attributes_buildup_play_passing team_Attributes.buildup_play_passing
    team_attributes_buildup_play_passing_class team_Attributes.buildup_play_passing_class
    team_attributes_buildup_play_positioning_class team_Attributes.buildup_play_positioning_class
    team_attributes_chance_creation_passing team_Attributes.chance_creation_passing
    team_attributes_chance_creation_passing_class team_Attributes.chance_creation_passing_class
    team_attributes_chance_creation_crossing team_Attributes.chance_creation_crossing
    team_attributes_chance_creation_crossing_class team_Attributes.chance_creation_crossing_class
    team_attributes_chance_creation_shooting team_Attributes.chance_creation_shooting
    team_attributes_chance_creation_shooting_class team_Attributes.chance_creation_shooting_class
    team_attributes_chance_creation_positioning_class team_Attributes.chance_creation_positioning_class
    team_attributes_defence_pressure team_Attributes.defence_pressure
    team_attributes_defence_pressure_class team_Attributes.defence_pressure_class
    team_attributes_defence_aggression team_Attributes.defence_aggression
    team_attributes_defence_aggression_class team_Attributes.defence_aggression_class
    team_attributes_defence_team_width team_Attributes.defence_team_width
    team_attributes_defence_team_width_class team_Attributes.defence_team_width_class
    team_attributes_defence_defender_line_class team_Attributes.defence_defender_line_class
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
