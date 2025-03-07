using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("activity_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("activity_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "activity id"], Any[0, "activity name"], Any[1, "student id"], Any[1, "activity id"], Any[2, "faculty id"], Any[2, "activity id"], Any[3, "student id"], Any[3, "last name"], Any[3, "first name"], Any[3, "age"], Any[3, "sex"], Any[3, "major"], Any[3, "advisor"], Any[3, "city code"], Any[4, "faculty id"], Any[4, "last name"], Any[4, "first name"], Any[4, "rank"], Any[4, "sex"], Any[4, "phone"], Any[4, "room"], Any[4, "building"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "activity id"], Any[0, "activity name"], Any[1, "student id"], Any[1, "activity id"], Any[2, "faculty id"], Any[2, "activity id"], Any[3, "student id"], Any[3, "last name"], Any[3, "first name"], Any[3, "age"], Any[3, "sex"], Any[3, "major"], Any[3, "advisor"], Any[3, "city code"], Any[4, "faculty id"], Any[4, "last name"], Any[4, "first name"], Any[4, "rank"], Any[4, "sex"], Any[4, "phone"], Any[4, "room"], Any[4, "building"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "activity id"], Any[0, "activity name"], Any[1, "student id"], Any[1, "activity id"], Any[2, "faculty id"], Any[2, "activity id"], Any[3, "student id"], Any[3, "last name"], Any[3, "first name"], Any[3, "age"], Any[3, "sex"], Any[3, "major"], Any[3, "advisor"], Any[3, "city code"], Any[4, "faculty id"], Any[4, "last name"], Any[4, "first name"], Any[4, "rank"], Any[4, "sex"], Any[4, "phone"], Any[4, "room"], Any[4, "building"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "activity id"], Any[0, "activity name"], Any[1, "student id"], Any[1, "activity id"], Any[2, "faculty id"], Any[2, "activity id"], Any[3, "student id"], Any[3, "last name"], Any[3, "first name"], Any[3, "age"], Any[3, "sex"], Any[3, "major"], Any[3, "advisor"], Any[3, "city code"], Any[4, "faculty id"], Any[4, "last name"], Any[4, "first name"], Any[4, "rank"], Any[4, "sex"], Any[4, "phone"], Any[4, "room"], Any[4, "building"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "activity id"], Any[0, "activity name"], Any[1, "student id"], Any[1, "activity id"], Any[2, "faculty id"], Any[2, "activity id"], Any[3, "student id"], Any[3, "last name"], Any[3, "first name"], Any[3, "age"], Any[3, "sex"], Any[3, "major"], Any[3, "advisor"], Any[3, "city code"], Any[4, "faculty id"], Any[4, "last name"], Any[4, "first name"], Any[4, "rank"], Any[4, "sex"], Any[4, "phone"], Any[4, "room"], Any[4, "building"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[4, 1], Any[3, 7], Any[6, 1], Any[5, 15]])
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







PClean.@model Activity1Model begin
    @class Activity begin
        activity_name ~ ChooseUniformly(possibilities[:activity_name])
    end

    @class Student begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Faculty begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        rank ~ ChooseUniformly(possibilities[:rank])
        sex ~ ChooseUniformly(possibilities[:sex])
        phone ~ ChooseUniformly(possibilities[:phone])
        room ~ ChooseUniformly(possibilities[:room])
        building ~ ChooseUniformly(possibilities[:building])
    end

    @class Participates_in begin
        student ~ Student
        activity ~ Activity
    end

    @class Faculty_participates_in begin
        faculty ~ Faculty
        activity ~ Activity
    end

    @class Obs begin
        participates_in ~ Participates_in
        faculty_participates_in ~ Faculty_participates_in
    end
end

query = @query Activity1Model.Obs [
    activity_id participates_in.activity.activity_id
    activity_name participates_in.activity.activity_name
    student_id participates_in.student.student_id
    student_last_name participates_in.student.last_name
    student_first_name participates_in.student.first_name
    student_age participates_in.student.age
    student_sex participates_in.student.sex
    student_major participates_in.student.major
    student_advisor participates_in.student.advisor
    student_city_code participates_in.student.city_code
    faculty_id faculty_participates_in.faculty.faculty_id
    faculty_last_name faculty_participates_in.faculty.last_name
    faculty_first_name faculty_participates_in.faculty.first_name
    faculty_rank faculty_participates_in.faculty.rank
    faculty_sex faculty_participates_in.faculty.sex
    faculty_phone faculty_participates_in.faculty.phone
    faculty_room faculty_participates_in.faculty.room
    faculty_building faculty_participates_in.faculty.building
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
