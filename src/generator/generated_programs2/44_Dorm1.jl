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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "dorm id"], Any[1, "dorm name"], Any[1, "student capacity"], Any[1, "gender"], Any[2, "amenity id"], Any[2, "amenity name"], Any[3, "dorm id"], Any[3, "amenity id"], Any[4, "student id"], Any[4, "dorm id"], Any[4, "room number"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "dorm id"], Any[1, "dorm name"], Any[1, "student capacity"], Any[1, "gender"], Any[2, "amenity id"], Any[2, "amenity name"], Any[3, "dorm id"], Any[3, "amenity id"], Any[4, "student id"], Any[4, "dorm id"], Any[4, "room number"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
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
cols = Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "dorm id"], Any[1, "dorm name"], Any[1, "student capacity"], Any[1, "gender"], Any[2, "amenity id"], Any[2, "amenity name"], Any[3, "dorm id"], Any[3, "amenity id"], Any[4, "student id"], Any[4, "dorm id"], Any[4, "room number"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[16, 13], Any[15, 9], Any[18, 9], Any[17, 1]])
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







PClean.@model Dorm1Model begin
    @class Student begin
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

    @class Dorm_amenity begin
        amenity_id ~ Unmodeled()
        amenity_name ~ ChooseUniformly(possibilities[:amenity_name])
    end

    @class Has_amenity begin
        dorm ~ Dorm
        dorm_amenity ~ Dorm_amenity
    end

    @class Lives_in begin
        student ~ Student
        dorm ~ Dorm
        room_number ~ ChooseUniformly(possibilities[:room_number])
    end

    @class Obs begin
        has_amenity ~ Has_amenity
        lives_in ~ Lives_in
    end
end

query = @query Dorm1Model.Obs [
    student_id lives_in.student.student_id
    student_last_name lives_in.student.last_name
    student_first_name lives_in.student.first_name
    student_age lives_in.student.age
    student_sex lives_in.student.sex
    student_major lives_in.student.major
    student_advisor lives_in.student.advisor
    student_city_code lives_in.student.city_code
    dorm_id has_amenity.dorm.dorm_id
    dorm_name has_amenity.dorm.dorm_name
    dorm_student_capacity has_amenity.dorm.student_capacity
    dorm_gender has_amenity.dorm.gender
    dorm_amenity_amenity_id has_amenity.dorm_amenity.amenity_id
    dorm_amenity_amenity_name has_amenity.dorm_amenity.amenity_name
    lives_in_room_number lives_in.room_number
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
