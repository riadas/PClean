using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("course_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("course_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "course id"], Any[0, "staring date"], Any[0, "course"], Any[1, "teacher id"], Any[1, "name"], Any[1, "age"], Any[1, "hometown"], Any[2, "course id"], Any[2, "teacher id"], Any[2, "grade"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 4], Any[8, 1]])
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







PClean.@model CourseTeachModel begin
    @class Course begin
        staring_date ~ ChooseUniformly(possibilities[:staring_date])
        course ~ ChooseUniformly(possibilities[:course])
    end

    @class Teacher begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        hometown ~ ChooseUniformly(possibilities[:hometown])
    end

    @class Course_arrange begin
        teacher ~ Teacher
        grade ~ ChooseUniformly(possibilities[:grade])
    end

    @class Obs begin
        course_arrange ~ Course_arrange
    end
end

query = @query CourseTeachModel.Obs [
    course_id course_arrange.course.course_id
    course_staring_date course_arrange.course.staring_date
    course course_arrange.course.course
    teacher_id course_arrange.teacher.teacher_id
    teacher_name course_arrange.teacher.name
    teacher_age course_arrange.teacher.age
    teacher_hometown course_arrange.teacher.hometown
    course_arrange_grade course_arrange.grade
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
