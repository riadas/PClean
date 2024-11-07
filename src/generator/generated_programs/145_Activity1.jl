using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("activity_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("activity_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "activity id"], Any[0, "activity name"], Any[1, "student id"], Any[1, "activity id"], Any[2, "faculty id"], Any[2, "activity id"], Any[3, "student id"], Any[3, "last name"], Any[3, "first name"], Any[3, "age"], Any[3, "sex"], Any[3, "major"], Any[3, "advisor"], Any[3, "city code"], Any[4, "faculty id"], Any[4, "last name"], Any[4, "first name"], Any[4, "rank"], Any[4, "sex"], Any[4, "phone"], Any[4, "room"], Any[4, "building"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "activity id"], Any[0, "activity name"], Any[1, "student id"], Any[1, "activity id"], Any[2, "faculty id"], Any[2, "activity id"], Any[3, "student id"], Any[3, "last name"], Any[3, "first name"], Any[3, "age"], Any[3, "sex"], Any[3, "major"], Any[3, "advisor"], Any[3, "city code"], Any[4, "faculty id"], Any[4, "last name"], Any[4, "first name"], Any[4, "rank"], Any[4, "sex"], Any[4, "phone"], Any[4, "room"], Any[4, "building"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Activity1Model begin
    @class Activity begin
        activity_id ~ Unmodeled()
        activity_name ~ ChooseUniformly(possibilities[:activity_name])
    end

    @class Participates_In begin
        student_id ~ Unmodeled()
        activity_id ~ ChooseUniformly(possibilities[:activity_id])
    end

    @class Faculty_Participates_In begin
        faculty_id ~ Unmodeled()
        activity_id ~ ChooseUniformly(possibilities[:activity_id])
    end

    @class Student begin
        student_id ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Faculty begin
        faculty_id ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        rank ~ ChooseUniformly(possibilities[:rank])
        sex ~ ChooseUniformly(possibilities[:sex])
        phone ~ ChooseUniformly(possibilities[:phone])
        room ~ ChooseUniformly(possibilities[:room])
        building ~ ChooseUniformly(possibilities[:building])
    end

    @class Obs begin
        activity ~ Activity
        participates_In ~ Participates_In
        faculty_Participates_In ~ Faculty_Participates_In
        student ~ Student
        faculty ~ Faculty
    end
end

query = @query Activity1Model.Obs [
    activity_id activity.activity_id
    activity_name activity.activity_name
    student_id student.student_id
    student_last_name student.last_name
    student_first_name student.first_name
    student_age student.age
    student_sex student.sex
    student_major student.major
    student_advisor student.advisor
    student_city_code student.city_code
    faculty_id faculty.faculty_id
    faculty_last_name faculty.last_name
    faculty_first_name faculty.first_name
    faculty_rank faculty.rank
    faculty_sex faculty.sex
    faculty_phone faculty.phone
    faculty_room faculty.room
    faculty_building faculty.building
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
