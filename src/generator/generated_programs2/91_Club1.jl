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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "club id"], Any[1, "club name"], Any[1, "club description"], Any[1, "club location"], Any[2, "student id"], Any[2, "club id"], Any[2, "position"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "club id"], Any[1, "club name"], Any[1, "club description"], Any[1, "club location"], Any[2, "student id"], Any[2, "club id"], Any[2, "position"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "club id"], Any[1, "club name"], Any[1, "club description"], Any[1, "club location"], Any[2, "student id"], Any[2, "club id"], Any[2, "position"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "club id"], Any[1, "club name"], Any[1, "club description"], Any[1, "club location"], Any[2, "student id"], Any[2, "club id"], Any[2, "position"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "club id"], Any[1, "club name"], Any[1, "club description"], Any[1, "club location"], Any[2, "student id"], Any[2, "club id"], Any[2, "position"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[14, 9], Any[13, 1]])
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







PClean.@model Club1Model begin
    @class Student begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Club begin
        club_name ~ ChooseUniformly(possibilities[:club_name])
        club_description ~ ChooseUniformly(possibilities[:club_description])
        club_location ~ ChooseUniformly(possibilities[:club_location])
    end

    @class Member_of_club begin
        student ~ Student
        club ~ Club
        position ~ ChooseUniformly(possibilities[:position])
    end

    @class Obs begin
        member_of_club ~ Member_of_club
    end
end

query = @query Club1Model.Obs [
    student_id member_of_club.student.student_id
    student_last_name member_of_club.student.last_name
    student_first_name member_of_club.student.first_name
    student_age member_of_club.student.age
    student_sex member_of_club.student.sex
    student_major member_of_club.student.major
    student_advisor member_of_club.student.advisor
    student_city_code member_of_club.student.city_code
    club_id member_of_club.club.club_id
    club_name member_of_club.club.club_name
    club_description member_of_club.club.club_description
    club_location member_of_club.club.club_location
    member_of_club_position member_of_club.position
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
