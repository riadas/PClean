using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("school_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("school_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school"], Any[0, "location"], Any[0, "enrollment"], Any[0, "founded"], Any[0, "denomination"], Any[0, "boys or girls"], Any[0, "day or boarding"], Any[0, "year entered competition"], Any[0, "school colors"], Any[1, "school id"], Any[1, "nickname"], Any[1, "colors"], Any[1, "league"], Any[1, "class"], Any[1, "division"], Any[2, "school id"], Any[2, "school year"], Any[2, "class a"], Any[2, "class aa"], Any[3, "player id"], Any[3, "player"], Any[3, "team"], Any[3, "age"], Any[3, "position"], Any[3, "school id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school"], Any[0, "location"], Any[0, "enrollment"], Any[0, "founded"], Any[0, "denomination"], Any[0, "boys or girls"], Any[0, "day or boarding"], Any[0, "year entered competition"], Any[0, "school colors"], Any[1, "school id"], Any[1, "nickname"], Any[1, "colors"], Any[1, "league"], Any[1, "class"], Any[1, "division"], Any[2, "school id"], Any[2, "school year"], Any[2, "class a"], Any[2, "class aa"], Any[3, "player id"], Any[3, "player"], Any[3, "team"], Any[3, "age"], Any[3, "position"], Any[3, "school id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model SchoolPlayerModel begin
    @class School begin
        school_id ~ Unmodeled()
        school ~ ChooseUniformly(possibilities[:school])
        location ~ ChooseUniformly(possibilities[:location])
        enrollment ~ ChooseUniformly(possibilities[:enrollment])
        founded ~ ChooseUniformly(possibilities[:founded])
        denomination ~ ChooseUniformly(possibilities[:denomination])
        boys_or_girls ~ ChooseUniformly(possibilities[:boys_or_girls])
        day_or_boarding ~ ChooseUniformly(possibilities[:day_or_boarding])
        year_entered_competition ~ ChooseUniformly(possibilities[:year_entered_competition])
        school_colors ~ ChooseUniformly(possibilities[:school_colors])
    end

    @class School_Details begin
        school_id ~ Unmodeled()
        nickname ~ ChooseUniformly(possibilities[:nickname])
        colors ~ ChooseUniformly(possibilities[:colors])
        league ~ ChooseUniformly(possibilities[:league])
        class ~ ChooseUniformly(possibilities[:class])
        division ~ ChooseUniformly(possibilities[:division])
    end

    @class School_Performance begin
        school_id ~ Unmodeled()
        school_year ~ ChooseUniformly(possibilities[:school_year])
        class_a ~ ChooseUniformly(possibilities[:class_a])
        class_aa ~ ChooseUniformly(possibilities[:class_aa])
    end

    @class Player begin
        player_id ~ Unmodeled()
        player ~ ChooseUniformly(possibilities[:player])
        team ~ ChooseUniformly(possibilities[:team])
        age ~ ChooseUniformly(possibilities[:age])
        position ~ ChooseUniformly(possibilities[:position])
        school_id ~ ChooseUniformly(possibilities[:school_id])
    end

    @class Obs begin
        school ~ School
        school_Details ~ School_Details
        school_Performance ~ School_Performance
        player ~ Player
    end
end

query = @query SchoolPlayerModel.Obs [
    school_id school.school_id
    school school.school
    school_location school.location
    school_enrollment school.enrollment
    school_founded school.founded
    school_denomination school.denomination
    school_boys_or_girls school.boys_or_girls
    school_day_or_boarding school.day_or_boarding
    school_year_entered_competition school.year_entered_competition
    school_colors school.school_colors
    school_details_nickname school_Details.nickname
    school_details_colors school_Details.colors
    school_details_league school_Details.league
    school_details_class school_Details.class
    school_details_division school_Details.division
    school_performance_school_year school_Performance.school_year
    school_performance_class_a school_Performance.class_a
    school_performance_class_aa school_Performance.class_aa
    player_id player.player_id
    player player.player
    player_team player.team
    player_age player.age
    player_position player.position
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
