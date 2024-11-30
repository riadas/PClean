using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("basketball match_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("basketball match_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "team id"], Any[0, "school id"], Any[0, "team name"], Any[0, "acc regular season"], Any[0, "acc percent"], Any[0, "acc home"], Any[0, "acc road"], Any[0, "all games"], Any[0, "all games percent"], Any[0, "all home"], Any[0, "all road"], Any[0, "all neutral"], Any[1, "school id"], Any[1, "school"], Any[1, "location"], Any[1, "founded"], Any[1, "affiliation"], Any[1, "enrollment"], Any[1, "nickname"], Any[1, "primary conference"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "team id"], Any[0, "school id"], Any[0, "team name"], Any[0, "acc regular season"], Any[0, "acc percent"], Any[0, "acc home"], Any[0, "acc road"], Any[0, "all games"], Any[0, "all games percent"], Any[0, "all home"], Any[0, "all road"], Any[0, "all neutral"], Any[1, "school id"], Any[1, "school"], Any[1, "location"], Any[1, "founded"], Any[1, "affiliation"], Any[1, "enrollment"], Any[1, "nickname"], Any[1, "primary conference"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["school id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "team id"], Any[0, "team name"], Any[0, "acc regular season"], Any[0, "acc percent"], Any[0, "acc home"], Any[0, "acc road"], Any[0, "all games"], Any[0, "all games percent"], Any[0, "all home"], Any[0, "all road"], Any[0, "all neutral"], Any[1, "school"], Any[1, "location"], Any[1, "founded"], Any[1, "affiliation"], Any[1, "enrollment"], Any[1, "nickname"], Any[1, "primary conference"]]
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





PClean.@model UniversityBasketballModel begin
    @class University begin
        school_id ~ Unmodeled()
        school ~ ChooseUniformly(possibilities[:school])
        location ~ ChooseUniformly(possibilities[:location])
        founded ~ ChooseUniformly(possibilities[:founded])
        affiliation ~ ChooseUniformly(possibilities[:affiliation])
        enrollment ~ ChooseUniformly(possibilities[:enrollment])
        nickname ~ ChooseUniformly(possibilities[:nickname])
        primary_conference ~ ChooseUniformly(possibilities[:primary_conference])
    end

    @class Obs begin
        university ~ University
        team_id ~ Unmodeled()
        team_name ~ ChooseUniformly(possibilities[:team_name])
        acc_regular_season ~ ChooseUniformly(possibilities[:acc_regular_season])
        acc_percent ~ ChooseUniformly(possibilities[:acc_percent])
        acc_home ~ ChooseUniformly(possibilities[:acc_home])
        acc_road ~ ChooseUniformly(possibilities[:acc_road])
        all_games ~ ChooseUniformly(possibilities[:all_games])
        all_games_percent ~ ChooseUniformly(possibilities[:all_games_percent])
        all_home ~ ChooseUniformly(possibilities[:all_home])
        all_road ~ ChooseUniformly(possibilities[:all_road])
        all_neutral ~ ChooseUniformly(possibilities[:all_neutral])
    end
end

query = @query UniversityBasketballModel.Obs [
    basketball_match_team_id team_id
    basketball_match_team_name team_name
    basketball_match_acc_regular_season acc_regular_season
    basketball_match_acc_percent acc_percent
    basketball_match_acc_home acc_home
    basketball_match_acc_road acc_road
    basketball_match_all_games all_games
    basketball_match_all_games_percent all_games_percent
    basketball_match_all_home all_home
    basketball_match_all_road all_road
    basketball_match_all_neutral all_neutral
    university_school_id university.school_id
    university_school university.school
    university_location university.location
    university_founded university.founded
    university_affiliation university.affiliation
    university_enrollment university.enrollment
    university_nickname university.nickname
    university_primary_conference university.primary_conference
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
