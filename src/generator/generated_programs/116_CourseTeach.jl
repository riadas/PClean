using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("course_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("course_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "course id"], Any[0, "staring date"], Any[0, "course"], Any[1, "teacher id"], Any[1, "name"], Any[1, "age"], Any[1, "hometown"], Any[2, "course id"], Any[2, "teacher id"], Any[2, "grade"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "course id"], Any[0, "staring date"], Any[0, "course"], Any[1, "teacher id"], Any[1, "name"], Any[1, "age"], Any[1, "hometown"], Any[2, "course id"], Any[2, "teacher id"], Any[2, "grade"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CourseTeachModel begin
    @class Course begin
        course_id ~ Unmodeled()
        staring_date ~ ChooseUniformly(possibilities[:staring_date])
        course ~ ChooseUniformly(possibilities[:course])
    end

    @class Teacher begin
        teacher_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        hometown ~ ChooseUniformly(possibilities[:hometown])
    end

    @class Course_Arrange begin
        course_id ~ Unmodeled()
        teacher_id ~ ChooseUniformly(possibilities[:teacher_id])
        grade ~ ChooseUniformly(possibilities[:grade])
    end

    @class Obs begin
        course ~ Course
        teacher ~ Teacher
        course_Arrange ~ Course_Arrange
    end
end

query = @query CourseTeachModel.Obs [
    course_id course.course_id
    course_staring_date course.staring_date
    course course.course
    teacher_id teacher.teacher_id
    teacher_name teacher.name
    teacher_age teacher.age
    teacher_hometown teacher.hometown
    course_arrange_grade course_Arrange.grade
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
