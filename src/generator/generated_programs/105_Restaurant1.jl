using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("student_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("student_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "restaurant id"], Any[1, "restaurant name"], Any[1, "address"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "restaurant type id"], Any[3, "restaurant type id"], Any[3, "restaurant type name"], Any[3, "restaurant type description"], Any[4, "student id"], Any[4, "restaurant id"], Any[4, "time"], Any[4, "spent"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "student id"], Any[0, "last name"], Any[0, "first name"], Any[0, "age"], Any[0, "sex"], Any[0, "major"], Any[0, "advisor"], Any[0, "city code"], Any[1, "restaurant id"], Any[1, "restaurant name"], Any[1, "address"], Any[1, "rating"], Any[2, "restaurant id"], Any[2, "restaurant type id"], Any[3, "restaurant type id"], Any[3, "restaurant type name"], Any[3, "restaurant type description"], Any[4, "student id"], Any[4, "restaurant id"], Any[4, "time"], Any[4, "spent"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Restaurant1Model begin
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

    @class Restaurant begin
        restaurant_id ~ Unmodeled()
        restaurant_name ~ ChooseUniformly(possibilities[:restaurant_name])
        address ~ ChooseUniformly(possibilities[:address])
        rating ~ ChooseUniformly(possibilities[:rating])
    end

    @class Type_Of_Restaurant begin
        restaurant_id ~ Unmodeled()
        restaurant_type_id ~ ChooseUniformly(possibilities[:restaurant_type_id])
    end

    @class Restaurant_Type begin
        restaurant_type_id ~ Unmodeled()
        restaurant_type_name ~ ChooseUniformly(possibilities[:restaurant_type_name])
        restaurant_type_description ~ ChooseUniformly(possibilities[:restaurant_type_description])
    end

    @class Visits_Restaurant begin
        student_id ~ Unmodeled()
        restaurant_id ~ ChooseUniformly(possibilities[:restaurant_id])
        time ~ TimePrior(possibilities[:time])
        spent ~ ChooseUniformly(possibilities[:spent])
    end

    @class Obs begin
        student ~ Student
        restaurant ~ Restaurant
        type_Of_Restaurant ~ Type_Of_Restaurant
        restaurant_Type ~ Restaurant_Type
        visits_Restaurant ~ Visits_Restaurant
    end
end

query = @query Restaurant1Model.Obs [
    student_id student.student_id
    student_last_name student.last_name
    student_first_name student.first_name
    student_age student.age
    student_sex student.sex
    student_major student.major
    student_advisor student.advisor
    student_city_code student.city_code
    restaurant_id restaurant.restaurant_id
    restaurant_name restaurant.restaurant_name
    restaurant_address restaurant.address
    restaurant_rating restaurant.rating
    restaurant_type_id restaurant_Type.restaurant_type_id
    restaurant_type_name restaurant_Type.restaurant_type_name
    restaurant_type_description restaurant_Type.restaurant_type_description
    visits_restaurant_time visits_Restaurant.time
    visits_restaurant_spent visits_Restaurant.spent
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
