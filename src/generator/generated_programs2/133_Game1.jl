using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("student_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("student_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "game id"], Any[1, "game name"], Any[1, "game type"], Any[2, "student id"], Any[2, "game id"], Any[2, "hours played"], Any[3, "student id"], Any[3, "sport name"], Any[3, "hours per week"], Any[3, "games played"], Any[3, "on scholarship"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "game id"], Any[1, "game name"], Any[1, "game type"], Any[2, "student id"], Any[2, "game id"], Any[2, "hours played"], Any[3, "student id"], Any[3, "sport name"], Any[3, "hours per week"], Any[3, "games played"], Any[3, "on scholarship"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "game id"], Any[1, "game name"], Any[1, "game type"], Any[2, "student id"], Any[2, "game id"], Any[2, "hours played"], Any[3, "student id"], Any[3, "sport name"], Any[3, "hours per week"], Any[3, "games played"], Any[3, "on scholarship"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "game id"], Any[1, "game name"], Any[1, "game type"], Any[2, "student id"], Any[2, "game id"], Any[2, "hours played"], Any[3, "student id"], Any[3, "sport name"], Any[3, "hours per week"], Any[3, "games played"], Any[3, "on scholarship"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "game id"], Any[1, "game name"], Any[1, "game type"], Any[2, "student id"], Any[2, "game id"], Any[2, "hours played"], Any[3, "student id"], Any[3, "sport name"], Any[3, "hours per week"], Any[3, "games played"], Any[3, "on scholarship"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 1], Any[13, 9], Any[15, 1]])
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







PClean.@model Game1Model begin
    @class Student begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Video_games begin
        game_name ~ ChooseUniformly(possibilities[:game_name])
        game_type ~ ChooseUniformly(possibilities[:game_type])
    end

    @class Plays_games begin
        student ~ Student
        video_games ~ Video_games
        hours_played ~ ChooseUniformly(possibilities[:hours_played])
    end

    @class Sports_info begin
        student ~ Student
        sport_name ~ ChooseUniformly(possibilities[:sport_name])
        hours_per_week ~ ChooseUniformly(possibilities[:hours_per_week])
        games_played ~ ChooseUniformly(possibilities[:games_played])
        on_scholarship ~ ChooseUniformly(possibilities[:on_scholarship])
    end

    @class Obs begin
        plays_games ~ Plays_games
        sports_info ~ Sports_info
    end
end

query = @query Game1Model.Obs [
    student_id plays_games.student.student_id
    student_last_name plays_games.student.last_name
    student_first_name plays_games.student.first_name
    student_age plays_games.student.age
    student_sex plays_games.student.sex
    student_major plays_games.student.major
    student_advisor plays_games.student.advisor
    student_city_code plays_games.student.city_code
    video_games_game_id plays_games.video_games.game_id
    video_games_game_name plays_games.video_games.game_name
    video_games_game_type plays_games.video_games.game_type
    plays_games_hours_played plays_games.hours_played
    sports_info_sport_name sports_info.sport_name
    sports_info_hours_per_week sports_info.hours_per_week
    sports_info_games_played sports_info.games_played
    sports_info_on_scholarship sports_info.on_scholarship
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
