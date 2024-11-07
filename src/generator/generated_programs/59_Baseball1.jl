using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("all star_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("all star_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "player id"], Any[0, "year"], Any[0, "game num"], Any[0, "game id"], Any[0, "team id"], Any[0, "league id"], Any[0, "gp"], Any[0, "starting pos"], Any[1, "year"], Any[1, "team id"], Any[1, "league id"], Any[1, "player id"], Any[1, "g all"], Any[1, "gs"], Any[1, "g batting"], Any[1, "g defense"], Any[1, "g p"], Any[1, "g c"], Any[1, "g 1b"], Any[1, "g 2b"], Any[1, "g 3b"], Any[1, "g ss"], Any[1, "g lf"], Any[1, "g cf"], Any[1, "g rf"], Any[1, "g of"], Any[1, "g dh"], Any[1, "g ph"], Any[1, "g pr"], Any[2, "player id"], Any[2, "award id"], Any[2, "year"], Any[2, "league id"], Any[2, "tie"], Any[2, "notes"], Any[3, "player id"], Any[3, "award id"], Any[3, "year"], Any[3, "league id"], Any[3, "tie"], Any[3, "notes"], Any[4, "award id"], Any[4, "year"], Any[4, "league id"], Any[4, "player id"], Any[4, "points won"], Any[4, "points max"], Any[4, "votes first"], Any[5, "award id"], Any[5, "year"], Any[5, "league id"], Any[5, "player id"], Any[5, "points won"], Any[5, "points max"], Any[5, "votes first"], Any[6, "player id"], Any[6, "year"], Any[6, "stint"], Any[6, "team id"], Any[6, "league id"], Any[6, "g"], Any[6, "ab"], Any[6, "r"], Any[6, "h"], Any[6, "double"], Any[6, "triple"], Any[6, "hr"], Any[6, "rbi"], Any[6, "sb"], Any[6, "cs"], Any[6, "bb"], Any[6, "so"], Any[6, "ibb"], Any[6, "hbp"], Any[6, "sh"], Any[6, "sf"], Any[6, "g idp"], Any[7, "year"], Any[7, "round"], Any[7, "player id"], Any[7, "team id"], Any[7, "league id"], Any[7, "g"], Any[7, "ab"], Any[7, "r"], Any[7, "h"], Any[7, "double"], Any[7, "triple"], Any[7, "hr"], Any[7, "rbi"], Any[7, "sb"], Any[7, "cs"], Any[7, "bb"], Any[7, "so"], Any[7, "ibb"], Any[7, "hbp"], Any[7, "sh"], Any[7, "sf"], Any[7, "g idp"], Any[8, "player id"], Any[8, "college id"], Any[8, "year"], Any[9, "player id"], Any[9, "year"], Any[9, "stint"], Any[9, "team id"], Any[9, "league id"], Any[9, "pos"], Any[9, "g"], Any[9, "gs"], Any[9, "inn outs"], Any[9, "po"], Any[9, "a"], Any[9, "e"], Any[9, "dp"], Any[9, "pb"], Any[9, "wp"], Any[9, "sb"], Any[9, "cs"], Any[9, "zr"], Any[10, "player id"], Any[10, "year"], Any[10, "stint"], Any[10, "glf"], Any[10, "gcf"], Any[10, "grf"], Any[11, "player id"], Any[11, "year"], Any[11, "team id"], Any[11, "league id"], Any[11, "round"], Any[11, "pos"], Any[11, "g"], Any[11, "gs"], Any[11, "inn outs"], Any[11, "po"], Any[11, "a"], Any[11, "e"], Any[11, "dp"], Any[11, "tp"], Any[11, "pb"], Any[11, "sb"], Any[11, "cs"], Any[12, "player id"], Any[12, "yearid"], Any[12, "votedby"], Any[12, "ballots"], Any[12, "needed"], Any[12, "votes"], Any[12, "inducted"], Any[12, "category"], Any[12, "needed note"], Any[13, "year"], Any[13, "league id"], Any[13, "team id"], Any[13, "park id"], Any[13, "span first"], Any[13, "span last"], Any[13, "games"], Any[13, "openings"], Any[13, "attendance"], Any[14, "player id"], Any[14, "year"], Any[14, "team id"], Any[14, "league id"], Any[14, "inseason"], Any[14, "g"], Any[14, "w"], Any[14, "l"], Any[14, "rank"], Any[14, "plyr mgr"], Any[15, "player id"], Any[15, "year"], Any[15, "team id"], Any[15, "league id"], Any[15, "inseason"], Any[15, "half"], Any[15, "g"], Any[15, "w"], Any[15, "l"], Any[15, "rank"], Any[16, "player id"], Any[16, "birth year"], Any[16, "birth month"], Any[16, "birth day"], Any[16, "birth country"], Any[16, "birth state"], Any[16, "birth city"], Any[16, "death year"], Any[16, "death month"], Any[16, "death day"], Any[16, "death country"], Any[16, "death state"], Any[16, "death city"], Any[16, "name first"], Any[16, "name last"], Any[16, "name given"], Any[16, "weight"], Any[16, "height"], Any[16, "bats"], Any[16, "throws"], Any[16, "debut"], Any[16, "final game"], Any[16, "retro id"], Any[16, "bbref id"], Any[17, "park id"], Any[17, "park name"], Any[17, "park alias"], Any[17, "city"], Any[17, "state"], Any[17, "country"], Any[18, "player id"], Any[18, "year"], Any[18, "stint"], Any[18, "team id"], Any[18, "league id"], Any[18, "w"], Any[18, "l"], Any[18, "g"], Any[18, "gs"], Any[18, "cg"], Any[18, "sho"], Any[18, "sv"], Any[18, "ipouts"], Any[18, "h"], Any[18, "er"], Any[18, "hr"], Any[18, "bb"], Any[18, "so"], Any[18, "baopp"], Any[18, "era"], Any[18, "ibb"], Any[18, "wp"], Any[18, "hbp"], Any[18, "bk"], Any[18, "bfp"], Any[18, "gf"], Any[18, "r"], Any[18, "sh"], Any[18, "sf"], Any[18, "g idp"], Any[19, "player id"], Any[19, "year"], Any[19, "round"], Any[19, "team id"], Any[19, "league id"], Any[19, "w"], Any[19, "l"], Any[19, "g"], Any[19, "gs"], Any[19, "cg"], Any[19, "sho"], Any[19, "sv"], Any[19, "ipouts"], Any[19, "h"], Any[19, "er"], Any[19, "hr"], Any[19, "bb"], Any[19, "so"], Any[19, "baopp"], Any[19, "era"], Any[19, "ibb"], Any[19, "wp"], Any[19, "hbp"], Any[19, "bk"], Any[19, "bfp"], Any[19, "gf"], Any[19, "r"], Any[19, "sh"], Any[19, "sf"], Any[19, "g idp"], Any[20, "year"], Any[20, "team id"], Any[20, "league id"], Any[20, "player id"], Any[20, "salary"], Any[21, "college id"], Any[21, "name full"], Any[21, "city"], Any[21, "state"], Any[21, "country"], Any[22, "year"], Any[22, "round"], Any[22, "team id winner"], Any[22, "league id winner"], Any[22, "team id loser"], Any[22, "league id loser"], Any[22, "wins"], Any[22, "losses"], Any[22, "ties"], Any[23, "year"], Any[23, "league id"], Any[23, "team id"], Any[23, "franchise id"], Any[23, "div id"], Any[23, "rank"], Any[23, "g"], Any[23, "ghome"], Any[23, "w"], Any[23, "l"], Any[23, "div win"], Any[23, "wc win"], Any[23, "lg win"], Any[23, "ws win"], Any[23, "r"], Any[23, "ab"], Any[23, "h"], Any[23, "double"], Any[23, "triple"], Any[23, "hr"], Any[23, "bb"], Any[23, "so"], Any[23, "sb"], Any[23, "cs"], Any[23, "hbp"], Any[23, "sf"], Any[23, "ra"], Any[23, "er"], Any[23, "era"], Any[23, "cg"], Any[23, "sho"], Any[23, "sv"], Any[23, "ipouts"], Any[23, "ha"], Any[23, "hra"], Any[23, "bba"], Any[23, "soa"], Any[23, "e"], Any[23, "dp"], Any[23, "fp"], Any[23, "name"], Any[23, "park"], Any[23, "attendance"], Any[23, "bpf"], Any[23, "ppf"], Any[23, "team id br"], Any[23, "team id lahman45"], Any[23, "team id retro"], Any[24, "franchise id"], Any[24, "franchise name"], Any[24, "active"], Any[24, "na assoc"], Any[25, "year"], Any[25, "league id"], Any[25, "team id"], Any[25, "half"], Any[25, "div id"], Any[25, "div win"], Any[25, "rank"], Any[25, "g"], Any[25, "w"], Any[25, "l"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "player id"], Any[0, "year"], Any[0, "game num"], Any[0, "game id"], Any[0, "team id"], Any[0, "league id"], Any[0, "gp"], Any[0, "starting pos"], Any[1, "year"], Any[1, "team id"], Any[1, "league id"], Any[1, "player id"], Any[1, "g all"], Any[1, "gs"], Any[1, "g batting"], Any[1, "g defense"], Any[1, "g p"], Any[1, "g c"], Any[1, "g 1b"], Any[1, "g 2b"], Any[1, "g 3b"], Any[1, "g ss"], Any[1, "g lf"], Any[1, "g cf"], Any[1, "g rf"], Any[1, "g of"], Any[1, "g dh"], Any[1, "g ph"], Any[1, "g pr"], Any[2, "player id"], Any[2, "award id"], Any[2, "year"], Any[2, "league id"], Any[2, "tie"], Any[2, "notes"], Any[3, "player id"], Any[3, "award id"], Any[3, "year"], Any[3, "league id"], Any[3, "tie"], Any[3, "notes"], Any[4, "award id"], Any[4, "year"], Any[4, "league id"], Any[4, "player id"], Any[4, "points won"], Any[4, "points max"], Any[4, "votes first"], Any[5, "award id"], Any[5, "year"], Any[5, "league id"], Any[5, "player id"], Any[5, "points won"], Any[5, "points max"], Any[5, "votes first"], Any[6, "player id"], Any[6, "year"], Any[6, "stint"], Any[6, "team id"], Any[6, "league id"], Any[6, "g"], Any[6, "ab"], Any[6, "r"], Any[6, "h"], Any[6, "double"], Any[6, "triple"], Any[6, "hr"], Any[6, "rbi"], Any[6, "sb"], Any[6, "cs"], Any[6, "bb"], Any[6, "so"], Any[6, "ibb"], Any[6, "hbp"], Any[6, "sh"], Any[6, "sf"], Any[6, "g idp"], Any[7, "year"], Any[7, "round"], Any[7, "player id"], Any[7, "team id"], Any[7, "league id"], Any[7, "g"], Any[7, "ab"], Any[7, "r"], Any[7, "h"], Any[7, "double"], Any[7, "triple"], Any[7, "hr"], Any[7, "rbi"], Any[7, "sb"], Any[7, "cs"], Any[7, "bb"], Any[7, "so"], Any[7, "ibb"], Any[7, "hbp"], Any[7, "sh"], Any[7, "sf"], Any[7, "g idp"], Any[8, "player id"], Any[8, "college id"], Any[8, "year"], Any[9, "player id"], Any[9, "year"], Any[9, "stint"], Any[9, "team id"], Any[9, "league id"], Any[9, "pos"], Any[9, "g"], Any[9, "gs"], Any[9, "inn outs"], Any[9, "po"], Any[9, "a"], Any[9, "e"], Any[9, "dp"], Any[9, "pb"], Any[9, "wp"], Any[9, "sb"], Any[9, "cs"], Any[9, "zr"], Any[10, "player id"], Any[10, "year"], Any[10, "stint"], Any[10, "glf"], Any[10, "gcf"], Any[10, "grf"], Any[11, "player id"], Any[11, "year"], Any[11, "team id"], Any[11, "league id"], Any[11, "round"], Any[11, "pos"], Any[11, "g"], Any[11, "gs"], Any[11, "inn outs"], Any[11, "po"], Any[11, "a"], Any[11, "e"], Any[11, "dp"], Any[11, "tp"], Any[11, "pb"], Any[11, "sb"], Any[11, "cs"], Any[12, "player id"], Any[12, "yearid"], Any[12, "votedby"], Any[12, "ballots"], Any[12, "needed"], Any[12, "votes"], Any[12, "inducted"], Any[12, "category"], Any[12, "needed note"], Any[13, "year"], Any[13, "league id"], Any[13, "team id"], Any[13, "park id"], Any[13, "span first"], Any[13, "span last"], Any[13, "games"], Any[13, "openings"], Any[13, "attendance"], Any[14, "player id"], Any[14, "year"], Any[14, "team id"], Any[14, "league id"], Any[14, "inseason"], Any[14, "g"], Any[14, "w"], Any[14, "l"], Any[14, "rank"], Any[14, "plyr mgr"], Any[15, "player id"], Any[15, "year"], Any[15, "team id"], Any[15, "league id"], Any[15, "inseason"], Any[15, "half"], Any[15, "g"], Any[15, "w"], Any[15, "l"], Any[15, "rank"], Any[16, "player id"], Any[16, "birth year"], Any[16, "birth month"], Any[16, "birth day"], Any[16, "birth country"], Any[16, "birth state"], Any[16, "birth city"], Any[16, "death year"], Any[16, "death month"], Any[16, "death day"], Any[16, "death country"], Any[16, "death state"], Any[16, "death city"], Any[16, "name first"], Any[16, "name last"], Any[16, "name given"], Any[16, "weight"], Any[16, "height"], Any[16, "bats"], Any[16, "throws"], Any[16, "debut"], Any[16, "final game"], Any[16, "retro id"], Any[16, "bbref id"], Any[17, "park id"], Any[17, "park name"], Any[17, "park alias"], Any[17, "city"], Any[17, "state"], Any[17, "country"], Any[18, "player id"], Any[18, "year"], Any[18, "stint"], Any[18, "team id"], Any[18, "league id"], Any[18, "w"], Any[18, "l"], Any[18, "g"], Any[18, "gs"], Any[18, "cg"], Any[18, "sho"], Any[18, "sv"], Any[18, "ipouts"], Any[18, "h"], Any[18, "er"], Any[18, "hr"], Any[18, "bb"], Any[18, "so"], Any[18, "baopp"], Any[18, "era"], Any[18, "ibb"], Any[18, "wp"], Any[18, "hbp"], Any[18, "bk"], Any[18, "bfp"], Any[18, "gf"], Any[18, "r"], Any[18, "sh"], Any[18, "sf"], Any[18, "g idp"], Any[19, "player id"], Any[19, "year"], Any[19, "round"], Any[19, "team id"], Any[19, "league id"], Any[19, "w"], Any[19, "l"], Any[19, "g"], Any[19, "gs"], Any[19, "cg"], Any[19, "sho"], Any[19, "sv"], Any[19, "ipouts"], Any[19, "h"], Any[19, "er"], Any[19, "hr"], Any[19, "bb"], Any[19, "so"], Any[19, "baopp"], Any[19, "era"], Any[19, "ibb"], Any[19, "wp"], Any[19, "hbp"], Any[19, "bk"], Any[19, "bfp"], Any[19, "gf"], Any[19, "r"], Any[19, "sh"], Any[19, "sf"], Any[19, "g idp"], Any[20, "year"], Any[20, "team id"], Any[20, "league id"], Any[20, "player id"], Any[20, "salary"], Any[21, "college id"], Any[21, "name full"], Any[21, "city"], Any[21, "state"], Any[21, "country"], Any[22, "year"], Any[22, "round"], Any[22, "team id winner"], Any[22, "league id winner"], Any[22, "team id loser"], Any[22, "league id loser"], Any[22, "wins"], Any[22, "losses"], Any[22, "ties"], Any[23, "year"], Any[23, "league id"], Any[23, "team id"], Any[23, "franchise id"], Any[23, "div id"], Any[23, "rank"], Any[23, "g"], Any[23, "ghome"], Any[23, "w"], Any[23, "l"], Any[23, "div win"], Any[23, "wc win"], Any[23, "lg win"], Any[23, "ws win"], Any[23, "r"], Any[23, "ab"], Any[23, "h"], Any[23, "double"], Any[23, "triple"], Any[23, "hr"], Any[23, "bb"], Any[23, "so"], Any[23, "sb"], Any[23, "cs"], Any[23, "hbp"], Any[23, "sf"], Any[23, "ra"], Any[23, "er"], Any[23, "era"], Any[23, "cg"], Any[23, "sho"], Any[23, "sv"], Any[23, "ipouts"], Any[23, "ha"], Any[23, "hra"], Any[23, "bba"], Any[23, "soa"], Any[23, "e"], Any[23, "dp"], Any[23, "fp"], Any[23, "name"], Any[23, "park"], Any[23, "attendance"], Any[23, "bpf"], Any[23, "ppf"], Any[23, "team id br"], Any[23, "team id lahman45"], Any[23, "team id retro"], Any[24, "franchise id"], Any[24, "franchise name"], Any[24, "active"], Any[24, "na assoc"], Any[25, "year"], Any[25, "league id"], Any[25, "team id"], Any[25, "half"], Any[25, "div id"], Any[25, "div win"], Any[25, "rank"], Any[25, "g"], Any[25, "w"], Any[25, "l"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Baseball1Model begin
    @class All_Star begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        game_num ~ ChooseUniformly(possibilities[:game_num])
        game_id ~ ChooseUniformly(possibilities[:game_id])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        gp ~ ChooseUniformly(possibilities[:gp])
        starting_pos ~ ChooseUniformly(possibilities[:starting_pos])
    end

    @class Appearances begin
        year ~ ChooseUniformly(possibilities[:year])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        player_id ~ ChooseUniformly(possibilities[:player_id])
        g_all ~ ChooseUniformly(possibilities[:g_all])
        gs ~ ChooseUniformly(possibilities[:gs])
        g_batting ~ ChooseUniformly(possibilities[:g_batting])
        g_defense ~ ChooseUniformly(possibilities[:g_defense])
        g_p ~ ChooseUniformly(possibilities[:g_p])
        g_c ~ ChooseUniformly(possibilities[:g_c])
        g_1b ~ ChooseUniformly(possibilities[:g_1b])
        g_2b ~ ChooseUniformly(possibilities[:g_2b])
        g_3b ~ ChooseUniformly(possibilities[:g_3b])
        g_ss ~ ChooseUniformly(possibilities[:g_ss])
        g_lf ~ ChooseUniformly(possibilities[:g_lf])
        g_cf ~ ChooseUniformly(possibilities[:g_cf])
        g_rf ~ ChooseUniformly(possibilities[:g_rf])
        g_of ~ ChooseUniformly(possibilities[:g_of])
        g_dh ~ ChooseUniformly(possibilities[:g_dh])
        g_ph ~ ChooseUniformly(possibilities[:g_ph])
        g_pr ~ ChooseUniformly(possibilities[:g_pr])
    end

    @class Manager_Award begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        award_id ~ ChooseUniformly(possibilities[:award_id])
        year ~ ChooseUniformly(possibilities[:year])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        tie ~ ChooseUniformly(possibilities[:tie])
        notes ~ ChooseUniformly(possibilities[:notes])
    end

    @class Player_Award begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        award_id ~ ChooseUniformly(possibilities[:award_id])
        year ~ ChooseUniformly(possibilities[:year])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        tie ~ ChooseUniformly(possibilities[:tie])
        notes ~ ChooseUniformly(possibilities[:notes])
    end

    @class Manager_Award_Vote begin
        award_id ~ ChooseUniformly(possibilities[:award_id])
        year ~ ChooseUniformly(possibilities[:year])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        player_id ~ ChooseUniformly(possibilities[:player_id])
        points_won ~ ChooseUniformly(possibilities[:points_won])
        points_max ~ ChooseUniformly(possibilities[:points_max])
        votes_first ~ ChooseUniformly(possibilities[:votes_first])
    end

    @class Player_Award_Vote begin
        award_id ~ ChooseUniformly(possibilities[:award_id])
        year ~ ChooseUniformly(possibilities[:year])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        player_id ~ ChooseUniformly(possibilities[:player_id])
        points_won ~ ChooseUniformly(possibilities[:points_won])
        points_max ~ ChooseUniformly(possibilities[:points_max])
        votes_first ~ ChooseUniformly(possibilities[:votes_first])
    end

    @class Batting begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        stint ~ ChooseUniformly(possibilities[:stint])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        g ~ ChooseUniformly(possibilities[:g])
        ab ~ ChooseUniformly(possibilities[:ab])
        r ~ ChooseUniformly(possibilities[:r])
        h ~ ChooseUniformly(possibilities[:h])
        double ~ ChooseUniformly(possibilities[:double])
        triple ~ ChooseUniformly(possibilities[:triple])
        hr ~ ChooseUniformly(possibilities[:hr])
        rbi ~ ChooseUniformly(possibilities[:rbi])
        sb ~ ChooseUniformly(possibilities[:sb])
        cs ~ ChooseUniformly(possibilities[:cs])
        bb ~ ChooseUniformly(possibilities[:bb])
        so ~ ChooseUniformly(possibilities[:so])
        ibb ~ ChooseUniformly(possibilities[:ibb])
        hbp ~ ChooseUniformly(possibilities[:hbp])
        sh ~ ChooseUniformly(possibilities[:sh])
        sf ~ ChooseUniformly(possibilities[:sf])
        g_idp ~ ChooseUniformly(possibilities[:g_idp])
    end

    @class Batting_Postseason begin
        year ~ ChooseUniformly(possibilities[:year])
        round ~ ChooseUniformly(possibilities[:round])
        player_id ~ ChooseUniformly(possibilities[:player_id])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        g ~ ChooseUniformly(possibilities[:g])
        ab ~ ChooseUniformly(possibilities[:ab])
        r ~ ChooseUniformly(possibilities[:r])
        h ~ ChooseUniformly(possibilities[:h])
        double ~ ChooseUniformly(possibilities[:double])
        triple ~ ChooseUniformly(possibilities[:triple])
        hr ~ ChooseUniformly(possibilities[:hr])
        rbi ~ ChooseUniformly(possibilities[:rbi])
        sb ~ ChooseUniformly(possibilities[:sb])
        cs ~ ChooseUniformly(possibilities[:cs])
        bb ~ ChooseUniformly(possibilities[:bb])
        so ~ ChooseUniformly(possibilities[:so])
        ibb ~ ChooseUniformly(possibilities[:ibb])
        hbp ~ ChooseUniformly(possibilities[:hbp])
        sh ~ ChooseUniformly(possibilities[:sh])
        sf ~ ChooseUniformly(possibilities[:sf])
        g_idp ~ ChooseUniformly(possibilities[:g_idp])
    end

    @class Player_College begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        college_id ~ ChooseUniformly(possibilities[:college_id])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Fielding begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        stint ~ ChooseUniformly(possibilities[:stint])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        pos ~ ChooseUniformly(possibilities[:pos])
        g ~ ChooseUniformly(possibilities[:g])
        gs ~ ChooseUniformly(possibilities[:gs])
        inn_outs ~ ChooseUniformly(possibilities[:inn_outs])
        po ~ ChooseUniformly(possibilities[:po])
        a ~ ChooseUniformly(possibilities[:a])
        e ~ ChooseUniformly(possibilities[:e])
        dp ~ ChooseUniformly(possibilities[:dp])
        pb ~ ChooseUniformly(possibilities[:pb])
        wp ~ ChooseUniformly(possibilities[:wp])
        sb ~ ChooseUniformly(possibilities[:sb])
        cs ~ ChooseUniformly(possibilities[:cs])
        zr ~ ChooseUniformly(possibilities[:zr])
    end

    @class Fielding_Outfield begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        stint ~ ChooseUniformly(possibilities[:stint])
        glf ~ ChooseUniformly(possibilities[:glf])
        gcf ~ ChooseUniformly(possibilities[:gcf])
        grf ~ ChooseUniformly(possibilities[:grf])
    end

    @class Fielding_Postseason begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        round ~ ChooseUniformly(possibilities[:round])
        pos ~ ChooseUniformly(possibilities[:pos])
        g ~ ChooseUniformly(possibilities[:g])
        gs ~ ChooseUniformly(possibilities[:gs])
        inn_outs ~ ChooseUniformly(possibilities[:inn_outs])
        po ~ ChooseUniformly(possibilities[:po])
        a ~ ChooseUniformly(possibilities[:a])
        e ~ ChooseUniformly(possibilities[:e])
        dp ~ ChooseUniformly(possibilities[:dp])
        tp ~ ChooseUniformly(possibilities[:tp])
        pb ~ ChooseUniformly(possibilities[:pb])
        sb ~ ChooseUniformly(possibilities[:sb])
        cs ~ ChooseUniformly(possibilities[:cs])
    end

    @class Hall_Of_Fame begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        yearid ~ ChooseUniformly(possibilities[:yearid])
        votedby ~ ChooseUniformly(possibilities[:votedby])
        ballots ~ ChooseUniformly(possibilities[:ballots])
        needed ~ ChooseUniformly(possibilities[:needed])
        votes ~ ChooseUniformly(possibilities[:votes])
        inducted ~ ChooseUniformly(possibilities[:inducted])
        category ~ ChooseUniformly(possibilities[:category])
        needed_note ~ ChooseUniformly(possibilities[:needed_note])
    end

    @class Home_Game begin
        year ~ ChooseUniformly(possibilities[:year])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        park_id ~ ChooseUniformly(possibilities[:park_id])
        span_first ~ ChooseUniformly(possibilities[:span_first])
        span_last ~ ChooseUniformly(possibilities[:span_last])
        games ~ ChooseUniformly(possibilities[:games])
        openings ~ ChooseUniformly(possibilities[:openings])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end

    @class Manager begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        inseason ~ ChooseUniformly(possibilities[:inseason])
        g ~ ChooseUniformly(possibilities[:g])
        w ~ ChooseUniformly(possibilities[:w])
        l ~ ChooseUniformly(possibilities[:l])
        rank ~ ChooseUniformly(possibilities[:rank])
        plyr_mgr ~ ChooseUniformly(possibilities[:plyr_mgr])
    end

    @class Manager_Half begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        inseason ~ ChooseUniformly(possibilities[:inseason])
        half ~ ChooseUniformly(possibilities[:half])
        g ~ ChooseUniformly(possibilities[:g])
        w ~ ChooseUniformly(possibilities[:w])
        l ~ ChooseUniformly(possibilities[:l])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Player begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
        birth_month ~ ChooseUniformly(possibilities[:birth_month])
        birth_day ~ ChooseUniformly(possibilities[:birth_day])
        birth_country ~ ChooseUniformly(possibilities[:birth_country])
        birth_state ~ ChooseUniformly(possibilities[:birth_state])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        death_year ~ ChooseUniformly(possibilities[:death_year])
        death_month ~ ChooseUniformly(possibilities[:death_month])
        death_day ~ ChooseUniformly(possibilities[:death_day])
        death_country ~ ChooseUniformly(possibilities[:death_country])
        death_state ~ ChooseUniformly(possibilities[:death_state])
        death_city ~ ChooseUniformly(possibilities[:death_city])
        name_first ~ ChooseUniformly(possibilities[:name_first])
        name_last ~ ChooseUniformly(possibilities[:name_last])
        name_given ~ ChooseUniformly(possibilities[:name_given])
        weight ~ ChooseUniformly(possibilities[:weight])
        height ~ ChooseUniformly(possibilities[:height])
        bats ~ ChooseUniformly(possibilities[:bats])
        throws ~ ChooseUniformly(possibilities[:throws])
        debut ~ ChooseUniformly(possibilities[:debut])
        final_game ~ ChooseUniformly(possibilities[:final_game])
        retro_id ~ ChooseUniformly(possibilities[:retro_id])
        bbref_id ~ ChooseUniformly(possibilities[:bbref_id])
    end

    @class Park begin
        park_id ~ ChooseUniformly(possibilities[:park_id])
        park_name ~ ChooseUniformly(possibilities[:park_name])
        park_alias ~ ChooseUniformly(possibilities[:park_alias])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Pitching begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        stint ~ ChooseUniformly(possibilities[:stint])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        w ~ ChooseUniformly(possibilities[:w])
        l ~ ChooseUniformly(possibilities[:l])
        g ~ ChooseUniformly(possibilities[:g])
        gs ~ ChooseUniformly(possibilities[:gs])
        cg ~ ChooseUniformly(possibilities[:cg])
        sho ~ ChooseUniformly(possibilities[:sho])
        sv ~ ChooseUniformly(possibilities[:sv])
        ipouts ~ ChooseUniformly(possibilities[:ipouts])
        h ~ ChooseUniformly(possibilities[:h])
        er ~ ChooseUniformly(possibilities[:er])
        hr ~ ChooseUniformly(possibilities[:hr])
        bb ~ ChooseUniformly(possibilities[:bb])
        so ~ ChooseUniformly(possibilities[:so])
        baopp ~ ChooseUniformly(possibilities[:baopp])
        era ~ ChooseUniformly(possibilities[:era])
        ibb ~ ChooseUniformly(possibilities[:ibb])
        wp ~ ChooseUniformly(possibilities[:wp])
        hbp ~ ChooseUniformly(possibilities[:hbp])
        bk ~ ChooseUniformly(possibilities[:bk])
        bfp ~ ChooseUniformly(possibilities[:bfp])
        gf ~ ChooseUniformly(possibilities[:gf])
        r ~ ChooseUniformly(possibilities[:r])
        sh ~ ChooseUniformly(possibilities[:sh])
        sf ~ ChooseUniformly(possibilities[:sf])
        g_idp ~ ChooseUniformly(possibilities[:g_idp])
    end

    @class Pitching_Postseason begin
        player_id ~ ChooseUniformly(possibilities[:player_id])
        year ~ ChooseUniformly(possibilities[:year])
        round ~ ChooseUniformly(possibilities[:round])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        w ~ ChooseUniformly(possibilities[:w])
        l ~ ChooseUniformly(possibilities[:l])
        g ~ ChooseUniformly(possibilities[:g])
        gs ~ ChooseUniformly(possibilities[:gs])
        cg ~ ChooseUniformly(possibilities[:cg])
        sho ~ ChooseUniformly(possibilities[:sho])
        sv ~ ChooseUniformly(possibilities[:sv])
        ipouts ~ ChooseUniformly(possibilities[:ipouts])
        h ~ ChooseUniformly(possibilities[:h])
        er ~ ChooseUniformly(possibilities[:er])
        hr ~ ChooseUniformly(possibilities[:hr])
        bb ~ ChooseUniformly(possibilities[:bb])
        so ~ ChooseUniformly(possibilities[:so])
        baopp ~ ChooseUniformly(possibilities[:baopp])
        era ~ ChooseUniformly(possibilities[:era])
        ibb ~ ChooseUniformly(possibilities[:ibb])
        wp ~ ChooseUniformly(possibilities[:wp])
        hbp ~ ChooseUniformly(possibilities[:hbp])
        bk ~ ChooseUniformly(possibilities[:bk])
        bfp ~ ChooseUniformly(possibilities[:bfp])
        gf ~ ChooseUniformly(possibilities[:gf])
        r ~ ChooseUniformly(possibilities[:r])
        sh ~ ChooseUniformly(possibilities[:sh])
        sf ~ ChooseUniformly(possibilities[:sf])
        g_idp ~ ChooseUniformly(possibilities[:g_idp])
    end

    @class Salary begin
        year ~ ChooseUniformly(possibilities[:year])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        player_id ~ ChooseUniformly(possibilities[:player_id])
        salary ~ ChooseUniformly(possibilities[:salary])
    end

    @class College begin
        college_id ~ ChooseUniformly(possibilities[:college_id])
        name_full ~ ChooseUniformly(possibilities[:name_full])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Postseason begin
        year ~ ChooseUniformly(possibilities[:year])
        round ~ ChooseUniformly(possibilities[:round])
        team_id_winner ~ ChooseUniformly(possibilities[:team_id_winner])
        league_id_winner ~ ChooseUniformly(possibilities[:league_id_winner])
        team_id_loser ~ ChooseUniformly(possibilities[:team_id_loser])
        league_id_loser ~ ChooseUniformly(possibilities[:league_id_loser])
        wins ~ ChooseUniformly(possibilities[:wins])
        losses ~ ChooseUniformly(possibilities[:losses])
        ties ~ ChooseUniformly(possibilities[:ties])
    end

    @class Team begin
        year ~ ChooseUniformly(possibilities[:year])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        franchise_id ~ ChooseUniformly(possibilities[:franchise_id])
        div_id ~ ChooseUniformly(possibilities[:div_id])
        rank ~ ChooseUniformly(possibilities[:rank])
        g ~ ChooseUniformly(possibilities[:g])
        ghome ~ ChooseUniformly(possibilities[:ghome])
        w ~ ChooseUniformly(possibilities[:w])
        l ~ ChooseUniformly(possibilities[:l])
        div_win ~ ChooseUniformly(possibilities[:div_win])
        wc_win ~ ChooseUniformly(possibilities[:wc_win])
        lg_win ~ ChooseUniformly(possibilities[:lg_win])
        ws_win ~ ChooseUniformly(possibilities[:ws_win])
        r ~ ChooseUniformly(possibilities[:r])
        ab ~ ChooseUniformly(possibilities[:ab])
        h ~ ChooseUniformly(possibilities[:h])
        double ~ ChooseUniformly(possibilities[:double])
        triple ~ ChooseUniformly(possibilities[:triple])
        hr ~ ChooseUniformly(possibilities[:hr])
        bb ~ ChooseUniformly(possibilities[:bb])
        so ~ ChooseUniformly(possibilities[:so])
        sb ~ ChooseUniformly(possibilities[:sb])
        cs ~ ChooseUniformly(possibilities[:cs])
        hbp ~ ChooseUniformly(possibilities[:hbp])
        sf ~ ChooseUniformly(possibilities[:sf])
        ra ~ ChooseUniformly(possibilities[:ra])
        er ~ ChooseUniformly(possibilities[:er])
        era ~ ChooseUniformly(possibilities[:era])
        cg ~ ChooseUniformly(possibilities[:cg])
        sho ~ ChooseUniformly(possibilities[:sho])
        sv ~ ChooseUniformly(possibilities[:sv])
        ipouts ~ ChooseUniformly(possibilities[:ipouts])
        ha ~ ChooseUniformly(possibilities[:ha])
        hra ~ ChooseUniformly(possibilities[:hra])
        bba ~ ChooseUniformly(possibilities[:bba])
        soa ~ ChooseUniformly(possibilities[:soa])
        e ~ ChooseUniformly(possibilities[:e])
        dp ~ ChooseUniformly(possibilities[:dp])
        fp ~ ChooseUniformly(possibilities[:fp])
        name ~ ChooseUniformly(possibilities[:name])
        park ~ ChooseUniformly(possibilities[:park])
        attendance ~ ChooseUniformly(possibilities[:attendance])
        bpf ~ ChooseUniformly(possibilities[:bpf])
        ppf ~ ChooseUniformly(possibilities[:ppf])
        team_id_br ~ ChooseUniformly(possibilities[:team_id_br])
        team_id_lahman45 ~ ChooseUniformly(possibilities[:team_id_lahman45])
        team_id_retro ~ ChooseUniformly(possibilities[:team_id_retro])
    end

    @class Team_Franchise begin
        franchise_id ~ ChooseUniformly(possibilities[:franchise_id])
        franchise_name ~ ChooseUniformly(possibilities[:franchise_name])
        active ~ ChooseUniformly(possibilities[:active])
        na_assoc ~ ChooseUniformly(possibilities[:na_assoc])
    end

    @class Team_Half begin
        year ~ ChooseUniformly(possibilities[:year])
        league_id ~ ChooseUniformly(possibilities[:league_id])
        team_id ~ ChooseUniformly(possibilities[:team_id])
        half ~ ChooseUniformly(possibilities[:half])
        div_id ~ ChooseUniformly(possibilities[:div_id])
        div_win ~ ChooseUniformly(possibilities[:div_win])
        rank ~ ChooseUniformly(possibilities[:rank])
        g ~ ChooseUniformly(possibilities[:g])
        w ~ ChooseUniformly(possibilities[:w])
        l ~ ChooseUniformly(possibilities[:l])
    end

    @class Obs begin
        all_Star ~ All_Star
        appearances ~ Appearances
        manager_Award ~ Manager_Award
        player_Award ~ Player_Award
        manager_Award_Vote ~ Manager_Award_Vote
        player_Award_Vote ~ Player_Award_Vote
        batting ~ Batting
        batting_Postseason ~ Batting_Postseason
        player_College ~ Player_College
        fielding ~ Fielding
        fielding_Outfield ~ Fielding_Outfield
        fielding_Postseason ~ Fielding_Postseason
        hall_Of_Fame ~ Hall_Of_Fame
        home_Game ~ Home_Game
        manager ~ Manager
        manager_Half ~ Manager_Half
        player ~ Player
        park ~ Park
        pitching ~ Pitching
        pitching_Postseason ~ Pitching_Postseason
        salary ~ Salary
        college ~ College
        postseason ~ Postseason
        team ~ Team
        team_Franchise ~ Team_Franchise
        team_Half ~ Team_Half
    end
end

query = @query Baseball1Model.Obs [
    all_star_year all_Star.year
    all_star_game_num all_Star.game_num
    all_star_game_id all_Star.game_id
    all_star_team_id all_Star.team_id
    all_star_league_id all_Star.league_id
    all_star_gp all_Star.gp
    all_star_starting_pos all_Star.starting_pos
    appearances_year appearances.year
    appearances_league_id appearances.league_id
    appearances_g_all appearances.g_all
    appearances_gs appearances.gs
    appearances_g_batting appearances.g_batting
    appearances_g_defense appearances.g_defense
    appearances_g_p appearances.g_p
    appearances_g_c appearances.g_c
    appearances_g_1b appearances.g_1b
    appearances_g_2b appearances.g_2b
    appearances_g_3b appearances.g_3b
    appearances_g_ss appearances.g_ss
    appearances_g_lf appearances.g_lf
    appearances_g_cf appearances.g_cf
    appearances_g_rf appearances.g_rf
    appearances_g_of appearances.g_of
    appearances_g_dh appearances.g_dh
    appearances_g_ph appearances.g_ph
    appearances_g_pr appearances.g_pr
    manager_award_award_id manager_Award.award_id
    manager_award_year manager_Award.year
    manager_award_league_id manager_Award.league_id
    manager_award_tie manager_Award.tie
    manager_award_notes manager_Award.notes
    player_award_award_id player_Award.award_id
    player_award_year player_Award.year
    player_award_league_id player_Award.league_id
    player_award_tie player_Award.tie
    player_award_notes player_Award.notes
    manager_award_vote_award_id manager_Award_Vote.award_id
    manager_award_vote_year manager_Award_Vote.year
    manager_award_vote_league_id manager_Award_Vote.league_id
    manager_award_vote_player_id manager_Award_Vote.player_id
    manager_award_vote_points_won manager_Award_Vote.points_won
    manager_award_vote_points_max manager_Award_Vote.points_max
    manager_award_vote_votes_first manager_Award_Vote.votes_first
    player_award_vote_award_id player_Award_Vote.award_id
    player_award_vote_year player_Award_Vote.year
    player_award_vote_league_id player_Award_Vote.league_id
    player_award_vote_points_won player_Award_Vote.points_won
    player_award_vote_points_max player_Award_Vote.points_max
    player_award_vote_votes_first player_Award_Vote.votes_first
    batting_year batting.year
    batting_stint batting.stint
    batting_team_id batting.team_id
    batting_league_id batting.league_id
    batting_g batting.g
    batting_ab batting.ab
    batting_r batting.r
    batting_h batting.h
    batting_double batting.double
    batting_triple batting.triple
    batting_hr batting.hr
    batting_rbi batting.rbi
    batting_sb batting.sb
    batting_cs batting.cs
    batting_bb batting.bb
    batting_so batting.so
    batting_ibb batting.ibb
    batting_hbp batting.hbp
    batting_sh batting.sh
    batting_sf batting.sf
    batting_g_idp batting.g_idp
    batting_postseason_year batting_Postseason.year
    batting_postseason_round batting_Postseason.round
    batting_postseason_league_id batting_Postseason.league_id
    batting_postseason_g batting_Postseason.g
    batting_postseason_ab batting_Postseason.ab
    batting_postseason_r batting_Postseason.r
    batting_postseason_h batting_Postseason.h
    batting_postseason_double batting_Postseason.double
    batting_postseason_triple batting_Postseason.triple
    batting_postseason_hr batting_Postseason.hr
    batting_postseason_rbi batting_Postseason.rbi
    batting_postseason_sb batting_Postseason.sb
    batting_postseason_cs batting_Postseason.cs
    batting_postseason_bb batting_Postseason.bb
    batting_postseason_so batting_Postseason.so
    batting_postseason_ibb batting_Postseason.ibb
    batting_postseason_hbp batting_Postseason.hbp
    batting_postseason_sh batting_Postseason.sh
    batting_postseason_sf batting_Postseason.sf
    batting_postseason_g_idp batting_Postseason.g_idp
    player_college_year player_College.year
    fielding_year fielding.year
    fielding_stint fielding.stint
    fielding_team_id fielding.team_id
    fielding_league_id fielding.league_id
    fielding_pos fielding.pos
    fielding_g fielding.g
    fielding_gs fielding.gs
    fielding_inn_outs fielding.inn_outs
    fielding_po fielding.po
    fielding_a fielding.a
    fielding_e fielding.e
    fielding_dp fielding.dp
    fielding_pb fielding.pb
    fielding_wp fielding.wp
    fielding_sb fielding.sb
    fielding_cs fielding.cs
    fielding_zr fielding.zr
    fielding_outfield_year fielding_Outfield.year
    fielding_outfield_stint fielding_Outfield.stint
    fielding_outfield_glf fielding_Outfield.glf
    fielding_outfield_gcf fielding_Outfield.gcf
    fielding_outfield_grf fielding_Outfield.grf
    fielding_postseason_year fielding_Postseason.year
    fielding_postseason_team_id fielding_Postseason.team_id
    fielding_postseason_league_id fielding_Postseason.league_id
    fielding_postseason_round fielding_Postseason.round
    fielding_postseason_pos fielding_Postseason.pos
    fielding_postseason_g fielding_Postseason.g
    fielding_postseason_gs fielding_Postseason.gs
    fielding_postseason_inn_outs fielding_Postseason.inn_outs
    fielding_postseason_po fielding_Postseason.po
    fielding_postseason_a fielding_Postseason.a
    fielding_postseason_e fielding_Postseason.e
    fielding_postseason_dp fielding_Postseason.dp
    fielding_postseason_tp fielding_Postseason.tp
    fielding_postseason_pb fielding_Postseason.pb
    fielding_postseason_sb fielding_Postseason.sb
    fielding_postseason_cs fielding_Postseason.cs
    hall_of_fame_yearid hall_Of_Fame.yearid
    hall_of_fame_votedby hall_Of_Fame.votedby
    hall_of_fame_ballots hall_Of_Fame.ballots
    hall_of_fame_needed hall_Of_Fame.needed
    hall_of_fame_votes hall_Of_Fame.votes
    hall_of_fame_inducted hall_Of_Fame.inducted
    hall_of_fame_category hall_Of_Fame.category
    hall_of_fame_needed_note hall_Of_Fame.needed_note
    home_game_year home_Game.year
    home_game_league_id home_Game.league_id
    home_game_span_first home_Game.span_first
    home_game_span_last home_Game.span_last
    home_game_games home_Game.games
    home_game_openings home_Game.openings
    home_game_attendance home_Game.attendance
    manager_player_id manager.player_id
    manager_year manager.year
    manager_league_id manager.league_id
    manager_inseason manager.inseason
    manager_g manager.g
    manager_w manager.w
    manager_l manager.l
    manager_rank manager.rank
    manager_plyr_mgr manager.plyr_mgr
    manager_half_player_id manager_Half.player_id
    manager_half_year manager_Half.year
    manager_half_league_id manager_Half.league_id
    manager_half_inseason manager_Half.inseason
    manager_half_half manager_Half.half
    manager_half_g manager_Half.g
    manager_half_w manager_Half.w
    manager_half_l manager_Half.l
    manager_half_rank manager_Half.rank
    player_id player.player_id
    player_birth_year player.birth_year
    player_birth_month player.birth_month
    player_birth_day player.birth_day
    player_birth_country player.birth_country
    player_birth_state player.birth_state
    player_birth_city player.birth_city
    player_death_year player.death_year
    player_death_month player.death_month
    player_death_day player.death_day
    player_death_country player.death_country
    player_death_state player.death_state
    player_death_city player.death_city
    player_name_first player.name_first
    player_name_last player.name_last
    player_name_given player.name_given
    player_weight player.weight
    player_height player.height
    player_bats player.bats
    player_throws player.throws
    player_debut player.debut
    player_final_game player.final_game
    player_retro_id player.retro_id
    player_bbref_id player.bbref_id
    park_id park.park_id
    park_name park.park_name
    park_alias park.park_alias
    park_city park.city
    park_state park.state
    park_country park.country
    pitching_player_id pitching.player_id
    pitching_year pitching.year
    pitching_stint pitching.stint
    pitching_team_id pitching.team_id
    pitching_league_id pitching.league_id
    pitching_w pitching.w
    pitching_l pitching.l
    pitching_g pitching.g
    pitching_gs pitching.gs
    pitching_cg pitching.cg
    pitching_sho pitching.sho
    pitching_sv pitching.sv
    pitching_ipouts pitching.ipouts
    pitching_h pitching.h
    pitching_er pitching.er
    pitching_hr pitching.hr
    pitching_bb pitching.bb
    pitching_so pitching.so
    pitching_baopp pitching.baopp
    pitching_era pitching.era
    pitching_ibb pitching.ibb
    pitching_wp pitching.wp
    pitching_hbp pitching.hbp
    pitching_bk pitching.bk
    pitching_bfp pitching.bfp
    pitching_gf pitching.gf
    pitching_r pitching.r
    pitching_sh pitching.sh
    pitching_sf pitching.sf
    pitching_g_idp pitching.g_idp
    pitching_postseason_player_id pitching_Postseason.player_id
    pitching_postseason_year pitching_Postseason.year
    pitching_postseason_round pitching_Postseason.round
    pitching_postseason_team_id pitching_Postseason.team_id
    pitching_postseason_league_id pitching_Postseason.league_id
    pitching_postseason_w pitching_Postseason.w
    pitching_postseason_l pitching_Postseason.l
    pitching_postseason_g pitching_Postseason.g
    pitching_postseason_gs pitching_Postseason.gs
    pitching_postseason_cg pitching_Postseason.cg
    pitching_postseason_sho pitching_Postseason.sho
    pitching_postseason_sv pitching_Postseason.sv
    pitching_postseason_ipouts pitching_Postseason.ipouts
    pitching_postseason_h pitching_Postseason.h
    pitching_postseason_er pitching_Postseason.er
    pitching_postseason_hr pitching_Postseason.hr
    pitching_postseason_bb pitching_Postseason.bb
    pitching_postseason_so pitching_Postseason.so
    pitching_postseason_baopp pitching_Postseason.baopp
    pitching_postseason_era pitching_Postseason.era
    pitching_postseason_ibb pitching_Postseason.ibb
    pitching_postseason_wp pitching_Postseason.wp
    pitching_postseason_hbp pitching_Postseason.hbp
    pitching_postseason_bk pitching_Postseason.bk
    pitching_postseason_bfp pitching_Postseason.bfp
    pitching_postseason_gf pitching_Postseason.gf
    pitching_postseason_r pitching_Postseason.r
    pitching_postseason_sh pitching_Postseason.sh
    pitching_postseason_sf pitching_Postseason.sf
    pitching_postseason_g_idp pitching_Postseason.g_idp
    salary_year salary.year
    salary_team_id salary.team_id
    salary_league_id salary.league_id
    salary_player_id salary.player_id
    salary salary.salary
    college_id college.college_id
    college_name_full college.name_full
    college_city college.city
    college_state college.state
    college_country college.country
    postseason_year postseason.year
    postseason_round postseason.round
    postseason_team_id_winner postseason.team_id_winner
    postseason_league_id_winner postseason.league_id_winner
    postseason_team_id_loser postseason.team_id_loser
    postseason_league_id_loser postseason.league_id_loser
    postseason_wins postseason.wins
    postseason_losses postseason.losses
    postseason_ties postseason.ties
    team_year team.year
    team_league_id team.league_id
    team_id team.team_id
    team_franchise_id team.franchise_id
    team_div_id team.div_id
    team_rank team.rank
    team_g team.g
    team_ghome team.ghome
    team_w team.w
    team_l team.l
    team_div_win team.div_win
    team_wc_win team.wc_win
    team_lg_win team.lg_win
    team_ws_win team.ws_win
    team_r team.r
    team_ab team.ab
    team_h team.h
    team_double team.double
    team_triple team.triple
    team_hr team.hr
    team_bb team.bb
    team_so team.so
    team_sb team.sb
    team_cs team.cs
    team_hbp team.hbp
    team_sf team.sf
    team_ra team.ra
    team_er team.er
    team_era team.era
    team_cg team.cg
    team_sho team.sho
    team_sv team.sv
    team_ipouts team.ipouts
    team_ha team.ha
    team_hra team.hra
    team_bba team.bba
    team_soa team.soa
    team_e team.e
    team_dp team.dp
    team_fp team.fp
    team_name team.name
    team_park team.park
    team_attendance team.attendance
    team_bpf team.bpf
    team_ppf team.ppf
    team_id_br team.team_id_br
    team_id_lahman45 team.team_id_lahman45
    team_id_retro team.team_id_retro
    team_franchise_franchise_id team_Franchise.franchise_id
    team_franchise_franchise_name team_Franchise.franchise_name
    team_franchise_active team_Franchise.active
    team_franchise_na_assoc team_Franchise.na_assoc
    team_half_year team_Half.year
    team_half_league_id team_Half.league_id
    team_half_team_id team_Half.team_id
    team_half_half team_Half.half
    team_half_div_id team_Half.div_id
    team_half_div_win team_Half.div_win
    team_half_rank team_Half.rank
    team_half_g team_Half.g
    team_half_w team_Half.w
    team_half_l team_Half.l
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
