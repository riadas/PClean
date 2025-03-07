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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "student id"], Any[1, "registration date"], Any[1, "election cycle"], Any[1, "president vote"], Any[1, "vice president vote"], Any[1, "secretary vote"], Any[1, "treasurer vote"], Any[1, "class president vote"], Any[1, "class senator vote"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "student id"], Any[1, "registration date"], Any[1, "election cycle"], Any[1, "president vote"], Any[1, "vice president vote"], Any[1, "secretary vote"], Any[1, "treasurer vote"], Any[1, "class president vote"], Any[1, "class senator vote"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "student id"], Any[1, "registration date"], Any[1, "election cycle"], Any[1, "president vote"], Any[1, "vice president vote"], Any[1, "secretary vote"], Any[1, "treasurer vote"], Any[1, "class president vote"], Any[1, "class senator vote"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "student id"], Any[1, "registration date"], Any[1, "election cycle"], Any[1, "president vote"], Any[1, "vice president vote"], Any[1, "secretary vote"], Any[1, "treasurer vote"], Any[1, "class president vote"], Any[1, "class senator vote"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "student id"], Any[1, "registration date"], Any[1, "election cycle"], Any[1, "president vote"], Any[1, "vice president vote"], Any[1, "secretary vote"], Any[1, "treasurer vote"], Any[1, "class president vote"], Any[1, "class senator vote"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[17, 1], Any[16, 1], Any[15, 1], Any[14, 1], Any[13, 1], Any[12, 1], Any[9, 1]])
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







PClean.@model Voter2Model begin
    @class Student begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Voting_record begin
        student ~ Student
        registration_date ~ ChooseUniformly(possibilities[:registration_date])
        election_cycle ~ ChooseUniformly(possibilities[:election_cycle])
    end

    @class Obs begin
        voting_record ~ Voting_record
    end
end

query = @query Voter2Model.Obs [
    student_id voting_record.student.student_id
    student_last_name voting_record.student.last_name
    student_first_name voting_record.student.first_name
    student_age voting_record.student.age
    student_sex voting_record.student.sex
    student_major voting_record.student.major
    student_advisor voting_record.student.advisor
    student_city_code voting_record.student.city_code
    voting_record_registration_date voting_record.registration_date
    voting_record_election_cycle voting_record.election_cycle
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
