using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("school_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("school_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school"], Any[0, "location"], Any[0, "enrollment"], Any[0, "founded"], Any[0, "denomination"], Any[0, "boys or girls"], Any[0, "day or boarding"], Any[0, "year entered competition"], Any[0, "school colors"], Any[1, "school id"], Any[1, "nickname"], Any[1, "colors"], Any[1, "league"], Any[1, "class"], Any[1, "division"], Any[2, "school id"], Any[2, "school year"], Any[2, "class a"], Any[2, "class aa"], Any[3, "player id"], Any[3, "player"], Any[3, "team"], Any[3, "age"], Any[3, "position"], Any[3, "school id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school"], Any[0, "location"], Any[0, "enrollment"], Any[0, "founded"], Any[0, "denomination"], Any[0, "boys or girls"], Any[0, "day or boarding"], Any[0, "year entered competition"], Any[0, "school colors"], Any[1, "school id"], Any[1, "nickname"], Any[1, "colors"], Any[1, "league"], Any[1, "class"], Any[1, "division"], Any[2, "school id"], Any[2, "school year"], Any[2, "class a"], Any[2, "class aa"], Any[3, "player id"], Any[3, "player"], Any[3, "team"], Any[3, "age"], Any[3, "position"], Any[3, "school id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["school id", "school id", "school id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "school"], Any[0, "location"], Any[0, "enrollment"], Any[0, "founded"], Any[0, "denomination"], Any[0, "boys or girls"], Any[0, "day or boarding"], Any[0, "year entered competition"], Any[0, "school colors"], Any[1, "nickname"], Any[1, "colors"], Any[1, "league"], Any[1, "class"], Any[1, "division"], Any[2, "school year"], Any[2, "class a"], Any[2, "class aa"], Any[3, "player id"], Any[3, "player"], Any[3, "team"], Any[3, "age"], Any[3, "position"]]
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

    @class Obs begin
        school ~ School
        nickname ~ ChooseUniformly(possibilities[:nickname])
        colors ~ ChooseUniformly(possibilities[:colors])
        league ~ ChooseUniformly(possibilities[:league])
        class ~ ChooseUniformly(possibilities[:class])
        division ~ ChooseUniformly(possibilities[:division])
        school_year ~ ChooseUniformly(possibilities[:school_year])
        class_a ~ ChooseUniformly(possibilities[:class_a])
        class_aa ~ ChooseUniformly(possibilities[:class_aa])
        player_id ~ Unmodeled()
        player ~ ChooseUniformly(possibilities[:player])
        team ~ ChooseUniformly(possibilities[:team])
        age ~ ChooseUniformly(possibilities[:age])
        position ~ ChooseUniformly(possibilities[:position])
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
    school_details_nickname nickname
    school_details_colors colors
    school_details_league league
    school_details_class class
    school_details_division division
    school_performance_school_year school_year
    school_performance_class_a class_a
    school_performance_class_aa class_aa
    player_id player_id
    player player
    player_team team
    player_age age
    player_position position
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
