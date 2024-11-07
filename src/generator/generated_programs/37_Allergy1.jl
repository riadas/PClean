using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("allergy type_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("allergy type_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "allergy name"], Any[0, "allergy type"], Any[1, "stuid"], Any[1, "allergy"], Any[2, "stuid"], Any[2, "last name"], Any[2, "first name"], Any[2, "age"], Any[2, "sex"], Any[2, "major"], Any[2, "advisor"], Any[2, "city code"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "allergy name"], Any[0, "allergy type"], Any[1, "stuid"], Any[1, "allergy"], Any[2, "stuid"], Any[2, "last name"], Any[2, "first name"], Any[2, "age"], Any[2, "sex"], Any[2, "major"], Any[2, "advisor"], Any[2, "city code"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Allergy1Model begin
    @class Allergy_Type begin
        allergy_name ~ ChooseUniformly(possibilities[:allergy_name])
        allergy_type ~ ChooseUniformly(possibilities[:allergy_type])
    end

    @class Has_Allergy begin
        stuid ~ Unmodeled()
        allergy ~ ChooseUniformly(possibilities[:allergy])
    end

    @class Student begin
        stuid ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Obs begin
        allergy_Type ~ Allergy_Type
        has_Allergy ~ Has_Allergy
        student ~ Student
    end
end

query = @query Allergy1Model.Obs [
    allergy_type_allergy_name allergy_Type.allergy_name
    allergy_type allergy_Type.allergy_type
    student_stuid student.stuid
    student_last_name student.last_name
    student_first_name student.first_name
    student_age student.age
    student_sex student.sex
    student_major student.major
    student_advisor student.advisor
    student_city_code student.city_code
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
