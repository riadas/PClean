using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("allergy_type_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("allergy_type_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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
cols = Any[Any[-1, "*"], Any[0, "allergy name"], Any[0, "allergy type"], Any[1, "stuid"], Any[1, "allergy"], Any[2, "stuid"], Any[2, "last name"], Any[2, "first name"], Any[2, "age"], Any[2, "sex"], Any[2, "major"], Any[2, "advisor"], Any[2, "city code"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[4, 1], Any[3, 5]])
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







PClean.@model Allergy1Model begin
    @class Allergy_type begin
        allergy_name ~ ChooseUniformly(possibilities[:allergy_name])
        allergy_type ~ ChooseUniformly(possibilities[:allergy_type])
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

    @class Has_allergy begin
        student ~ Student
        allergy_type ~ Allergy_type
    end

    @class Obs begin
        has_allergy ~ Has_allergy
    end
end

query = @query Allergy1Model.Obs [
    allergy_type_allergy_name has_allergy.allergy_type.allergy_name
    allergy_type has_allergy.allergy_type.allergy_type
    student_last_name has_allergy.student.last_name
    student_first_name has_allergy.student.first_name
    student_age has_allergy.student.age
    student_sex has_allergy.student.sex
    student_major has_allergy.student.major
    student_advisor has_allergy.student.advisor
    student_city_code has_allergy.student.city_code
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
