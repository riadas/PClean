using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("course_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("course_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "course id"], Any[0, "staring date"], Any[0, "course"], Any[1, "teacher id"], Any[1, "name"], Any[1, "age"], Any[1, "hometown"], Any[2, "course id"], Any[2, "teacher id"], Any[2, "grade"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "course id"], Any[0, "staring date"], Any[0, "course"], Any[1, "teacher id"], Any[1, "name"], Any[1, "age"], Any[1, "hometown"], Any[2, "course id"], Any[2, "teacher id"], Any[2, "grade"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["teacher id", "course id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "staring date"], Any[0, "course"], Any[1, "name"], Any[1, "age"], Any[1, "hometown"], Any[2, "grade"]]
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

    @class Obs begin
        course ~ Course
        teacher ~ Teacher
        grade ~ ChooseUniformly(possibilities[:grade])
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
    course_arrange_grade grade
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
