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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "restaurant id"], Any[1, "restaurant name"], Any[1, "address"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "restaurant type id"], Any[3, "restaurant type id"], Any[3, "restaurant type name"], Any[3, "restaurant type description"], Any[4, "student id"], Any[4, "restaurant id"], Any[4, "time"], Any[4, "spent"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "restaurant id"], Any[1, "restaurant name"], Any[1, "address"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "restaurant type id"], Any[3, "restaurant type id"], Any[3, "restaurant type name"], Any[3, "restaurant type description"], Any[4, "student id"], Any[4, "restaurant id"], Any[4, "time"], Any[4, "spent"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "restaurant id"], Any[1, "restaurant name"], Any[1, "address"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "restaurant type id"], Any[3, "restaurant type id"], Any[3, "restaurant type name"], Any[3, "restaurant type description"], Any[4, "student id"], Any[4, "restaurant id"], Any[4, "time"], Any[4, "spent"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "restaurant id"], Any[1, "restaurant name"], Any[1, "address"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "restaurant type id"], Any[3, "restaurant type id"], Any[3, "restaurant type name"], Any[3, "restaurant type description"], Any[4, "student id"], Any[4, "restaurant id"], Any[4, "time"], Any[4, "spent"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "restaurant id"], Any[1, "restaurant name"], Any[1, "address"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "restaurant type id"], Any[3, "restaurant type id"], Any[3, "restaurant type name"], Any[3, "restaurant type description"], Any[4, "student id"], Any[4, "restaurant id"], Any[4, "time"], Any[4, "spent"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[14, 15], Any[13, 9], Any[19, 9], Any[18, 1]])
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







PClean.@model Restaurant1Model begin
    @class Student begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        age ~ ChooseUniformly(possibilities[:age])
        sex ~ ChooseUniformly(possibilities[:sex])
        major ~ ChooseUniformly(possibilities[:major])
        advisor ~ ChooseUniformly(possibilities[:advisor])
        city_code ~ ChooseUniformly(possibilities[:city_code])
    end

    @class Restaurant begin
        restaurant_name ~ ChooseUniformly(possibilities[:restaurant_name])
        address ~ ChooseUniformly(possibilities[:address])
        rating ~ ChooseUniformly(possibilities[:rating])
    end

    @class Restaurant_type begin
        restaurant_type_name ~ ChooseUniformly(possibilities[:restaurant_type_name])
        restaurant_type_description ~ ChooseUniformly(possibilities[:restaurant_type_description])
    end

    @class Visits_restaurant begin
        student ~ Student
        restaurant ~ Restaurant
        time ~ TimePrior(possibilities[:time])
        spent ~ ChooseUniformly(possibilities[:spent])
    end

    @class Type_of_restaurant begin
        restaurant ~ Restaurant
        restaurant_type ~ Restaurant_type
    end

    @class Obs begin
        type_of_restaurant ~ Type_of_restaurant
        visits_restaurant ~ Visits_restaurant
    end
end

query = @query Restaurant1Model.Obs [
    student_id visits_restaurant.student.student_id
    student_last_name visits_restaurant.student.last_name
    student_first_name visits_restaurant.student.first_name
    student_age visits_restaurant.student.age
    student_sex visits_restaurant.student.sex
    student_major visits_restaurant.student.major
    student_advisor visits_restaurant.student.advisor
    student_city_code visits_restaurant.student.city_code
    restaurant_id type_of_restaurant.restaurant.restaurant_id
    restaurant_name type_of_restaurant.restaurant.restaurant_name
    restaurant_address type_of_restaurant.restaurant.address
    restaurant_rating type_of_restaurant.restaurant.rating
    restaurant_type_id type_of_restaurant.restaurant_type.restaurant_type_id
    restaurant_type_name type_of_restaurant.restaurant_type.restaurant_type_name
    restaurant_type_description type_of_restaurant.restaurant_type.restaurant_type_description
    visits_restaurant_time visits_restaurant.time
    visits_restaurant_spent visits_restaurant.spent
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
