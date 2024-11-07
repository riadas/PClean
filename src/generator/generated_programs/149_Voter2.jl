using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("student_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("student_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "student id"], Any[1, "registration date"], Any[1, "election cycle"], Any[1, "president vote"], Any[1, "vice president vote"], Any[1, "secretary vote"], Any[1, "treasurer vote"], Any[1, "class president vote"], Any[1, "class senator vote"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "student id"], Any[1, "registration date"], Any[1, "election cycle"], Any[1, "president vote"], Any[1, "vice president vote"], Any[1, "secretary vote"], Any[1, "treasurer vote"], Any[1, "class president vote"], Any[1, "class senator vote"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Voter2Model begin
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

    @class Voting_Record begin
        student_id ~ Unmodeled()
        registration_date ~ ChooseUniformly(possibilities[:registration_date])
        election_cycle ~ ChooseUniformly(possibilities[:election_cycle])
        president_vote ~ ChooseUniformly(possibilities[:president_vote])
        vice_president_vote ~ ChooseUniformly(possibilities[:vice_president_vote])
        secretary_vote ~ ChooseUniformly(possibilities[:secretary_vote])
        treasurer_vote ~ ChooseUniformly(possibilities[:treasurer_vote])
        class_president_vote ~ ChooseUniformly(possibilities[:class_president_vote])
        class_senator_vote ~ ChooseUniformly(possibilities[:class_senator_vote])
    end

    @class Obs begin
        student ~ Student
        voting_Record ~ Voting_Record
    end
end

query = @query Voter2Model.Obs [
    student_id student.student_id
    student_last_name student.last_name
    student_first_name student.first_name
    student_age student.age
    student_sex student.sex
    student_major student.major
    student_advisor student.advisor
    student_city_code student.city_code
    voting_record_registration_date voting_Record.registration_date
    voting_record_election_cycle voting_Record.election_cycle
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
