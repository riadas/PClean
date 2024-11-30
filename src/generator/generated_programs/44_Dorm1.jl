using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("student_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("student_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "dorm id"], Any[1, "dorm name"], Any[1, "student capacity"], Any[1, "gender"], Any[2, "amenity id"], Any[2, "amenity name"], Any[3, "dorm id"], Any[3, "amenity id"], Any[4, "student id"], Any[4, "dorm id"], Any[4, "room number"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "dorm id"], Any[1, "dorm name"], Any[1, "student capacity"], Any[1, "gender"], Any[2, "amenity id"], Any[2, "amenity name"], Any[3, "dorm id"], Any[3, "amenity id"], Any[4, "student id"], Any[4, "dorm id"], Any[4, "room number"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["amenity id", "dorm id", "dorm id", "student id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "dorm name"], Any[1, "student capacity"], Any[1, "gender"], Any[2, "amenity name"], Any[4, "room number"]]
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





PClean.@model Dorm1Model begin
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

    @class Dorm begin
        dorm_id ~ Unmodeled()
        dorm_name ~ ChooseUniformly(possibilities[:dorm_name])
        student_capacity ~ ChooseUniformly(possibilities[:student_capacity])
        gender ~ ChooseUniformly(possibilities[:gender])
    end

    @class Dorm_Amenity begin
        amenity_id ~ Unmodeled()
        amenity_name ~ ChooseUniformly(possibilities[:amenity_name])
    end

    @class Obs begin
        student ~ Student
        dorm ~ Dorm
        dorm_Amenity ~ Dorm_Amenity
        room_number ~ ChooseUniformly(possibilities[:room_number])
    end
end

query = @query Dorm1Model.Obs [
    student_id student.student_id
    student_last_name student.last_name
    student_first_name student.first_name
    student_age student.age
    student_sex student.sex
    student_major student.major
    student_advisor student.advisor
    student_city_code student.city_code
    dorm_id dorm.dorm_id
    dorm_name dorm.dorm_name
    dorm_student_capacity dorm.student_capacity
    dorm_gender dorm.gender
    dorm_amenity_amenity_id dorm_Amenity.amenity_id
    dorm_amenity_amenity_name dorm_Amenity.amenity_name
    lives_in_room_number room_number
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
