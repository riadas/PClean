using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("allergy type_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("allergy type_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "allergy name"], Any[0, "allergy type"], Any[1, "stuid"], Any[1, "allergy"], Any[2, "stuid"], Any[2, "last name"], Any[2, "first name"], Any[2, "age"], Any[2, "sex"], Any[2, "major"], Any[2, "advisor"], Any[2, "city code"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "allergy name"], Any[0, "allergy type"], Any[1, "stuid"], Any[1, "allergy"], Any[2, "stuid"], Any[2, "last name"], Any[2, "first name"], Any[2, "age"], Any[2, "sex"], Any[2, "major"], Any[2, "advisor"], Any[2, "city code"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["allergy", "stuid"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "allergy name"], Any[0, "allergy type"], Any[2, "last name"], Any[2, "first name"], Any[2, "age"], Any[2, "sex"], Any[2, "major"], Any[2, "advisor"], Any[2, "city code"]]
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





PClean.@model Allergy1Model begin
    @class Allergy_Type begin
        allergy_name ~ ChooseUniformly(possibilities[:allergy_name])
        allergy_type ~ ChooseUniformly(possibilities[:allergy_type])
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

    @class Has_Allergy begin
        student ~ Student
        allergy_Type ~ Allergy_Type
    end

    @class Obs begin
        has_Allergy ~ Has_Allergy
    end
end

query = @query Allergy1Model.Obs [
    allergy_type_allergy_name has_Allergy.allergy_Type.allergy_name
    allergy_type has_Allergy.allergy_Type.allergy_type
    student_stuid has_Allergy.student.stuid
    student_last_name has_Allergy.student.last_name
    student_first_name has_Allergy.student.first_name
    student_age has_Allergy.student.age
    student_sex has_Allergy.student.sex
    student_major has_Allergy.student.major
    student_advisor has_Allergy.student.advisor
    student_city_code has_Allergy.student.city_code
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
