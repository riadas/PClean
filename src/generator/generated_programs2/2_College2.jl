using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("classroom_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("classroom_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "building"], Any[0, "room number"], Any[0, "capacity"], Any[1, "department name"], Any[1, "building"], Any[1, "budget"], Any[2, "course id"], Any[2, "title"], Any[2, "department name"], Any[2, "credits"], Any[3, "id"], Any[3, "name"], Any[3, "department name"], Any[3, "salary"], Any[4, "course id"], Any[4, "section id"], Any[4, "semester"], Any[4, "year"], Any[4, "building"], Any[4, "room number"], Any[4, "time slot id"], Any[5, "id"], Any[5, "course id"], Any[5, "section id"], Any[5, "semester"], Any[5, "year"], Any[6, "id"], Any[6, "name"], Any[6, "department name"], Any[6, "total credits"], Any[7, "id"], Any[7, "course id"], Any[7, "section id"], Any[7, "semester"], Any[7, "year"], Any[7, "grade"], Any[8, "student id"], Any[8, "instructor id"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"], Any[10, "course id"], Any[10, "prerequisite id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "building"], Any[0, "room number"], Any[0, "capacity"], Any[1, "department name"], Any[1, "building"], Any[1, "budget"], Any[2, "course id"], Any[2, "title"], Any[2, "department name"], Any[2, "credits"], Any[3, "id"], Any[3, "name"], Any[3, "department name"], Any[3, "salary"], Any[4, "course id"], Any[4, "section id"], Any[4, "semester"], Any[4, "year"], Any[4, "building"], Any[4, "room number"], Any[4, "time slot id"], Any[5, "id"], Any[5, "course id"], Any[5, "section id"], Any[5, "semester"], Any[5, "year"], Any[6, "id"], Any[6, "name"], Any[6, "department name"], Any[6, "total credits"], Any[7, "id"], Any[7, "course id"], Any[7, "section id"], Any[7, "semester"], Any[7, "year"], Any[7, "grade"], Any[8, "student id"], Any[8, "instructor id"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"], Any[10, "course id"], Any[10, "prerequisite id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "building"], Any[0, "room number"], Any[0, "capacity"], Any[1, "department name"], Any[1, "building"], Any[1, "budget"], Any[2, "course id"], Any[2, "title"], Any[2, "department name"], Any[2, "credits"], Any[3, "id"], Any[3, "name"], Any[3, "department name"], Any[3, "salary"], Any[4, "course id"], Any[4, "section id"], Any[4, "semester"], Any[4, "year"], Any[4, "building"], Any[4, "room number"], Any[4, "time slot id"], Any[5, "id"], Any[5, "course id"], Any[5, "section id"], Any[5, "semester"], Any[5, "year"], Any[6, "id"], Any[6, "name"], Any[6, "department name"], Any[6, "total credits"], Any[7, "id"], Any[7, "course id"], Any[7, "section id"], Any[7, "semester"], Any[7, "year"], Any[7, "grade"], Any[8, "student id"], Any[8, "instructor id"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"], Any[10, "course id"], Any[10, "prerequisite id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "building"], Any[0, "room number"], Any[0, "capacity"], Any[1, "department name"], Any[1, "building"], Any[1, "budget"], Any[2, "course id"], Any[2, "title"], Any[2, "department name"], Any[2, "credits"], Any[3, "id"], Any[3, "name"], Any[3, "department name"], Any[3, "salary"], Any[4, "course id"], Any[4, "section id"], Any[4, "semester"], Any[4, "year"], Any[4, "building"], Any[4, "room number"], Any[4, "time slot id"], Any[5, "id"], Any[5, "course id"], Any[5, "section id"], Any[5, "semester"], Any[5, "year"], Any[6, "id"], Any[6, "name"], Any[6, "department name"], Any[6, "total credits"], Any[7, "id"], Any[7, "course id"], Any[7, "section id"], Any[7, "semester"], Any[7, "year"], Any[7, "grade"], Any[8, "student id"], Any[8, "instructor id"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"], Any[10, "course id"], Any[10, "prerequisite id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "building"], Any[0, "room number"], Any[0, "capacity"], Any[1, "department name"], Any[1, "building"], Any[1, "budget"], Any[2, "course id"], Any[2, "title"], Any[2, "department name"], Any[2, "credits"], Any[3, "id"], Any[3, "name"], Any[3, "department name"], Any[3, "salary"], Any[4, "course id"], Any[4, "section id"], Any[4, "semester"], Any[4, "year"], Any[4, "building"], Any[4, "room number"], Any[4, "time slot id"], Any[5, "id"], Any[5, "course id"], Any[5, "section id"], Any[5, "semester"], Any[5, "year"], Any[6, "id"], Any[6, "name"], Any[6, "department name"], Any[6, "total credits"], Any[7, "id"], Any[7, "course id"], Any[7, "section id"], Any[7, "semester"], Any[7, "year"], Any[7, "grade"], Any[8, "student id"], Any[8, "instructor id"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"], Any[10, "course id"], Any[10, "prerequisite id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 4], Any[13, 4], Any[19, 1], Any[20, 2], Any[15, 7], Any[22, 11], Any[23, 15], Any[24, 16], Any[25, 17], Any[26, 18], Any[29, 4], Any[31, 27], Any[32, 15], Any[33, 16], Any[34, 17], Any[35, 18], Any[37, 27], Any[38, 11], Any[46, 7], Any[45, 7]])
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







PClean.@model College2Model begin
    @class Classroom begin
        building ~ ChooseUniformly(possibilities[:building])
        room_number ~ ChooseUniformly(possibilities[:room_number])
        capacity ~ ChooseUniformly(possibilities[:capacity])
    end

    @class Department begin
        department_name ~ ChooseUniformly(possibilities[:department_name])
        building ~ ChooseUniformly(possibilities[:building])
        budget ~ ChooseUniformly(possibilities[:budget])
    end

    @class Course begin
        course_id ~ ChooseUniformly(possibilities[:course_id])
        title ~ ChooseUniformly(possibilities[:title])
        department ~ Department
        credits ~ ChooseUniformly(possibilities[:credits])
    end

    @class Instructor begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        department ~ Department
        salary ~ ChooseUniformly(possibilities[:salary])
    end

    @class Section begin
        course ~ Course
        section_id ~ ChooseUniformly(possibilities[:section_id])
        semester ~ ChooseUniformly(possibilities[:semester])
        year ~ ChooseUniformly(possibilities[:year])
        classroom ~ Classroom
        time_slot_id ~ ChooseUniformly(possibilities[:time_slot_id])
    end

    @class Teaches begin
        instructor ~ Instructor
        section ~ Section
    end

    @class Student begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        department ~ Department
        total_credits ~ ChooseUniformly(possibilities[:total_credits])
    end

    @class Takes_classes begin
        student ~ Student
        section ~ Section
        grade ~ ChooseUniformly(possibilities[:grade])
    end

    @class Advisor begin
        student ~ Student
        instructor ~ Instructor
    end

    @class Time_slot begin
        time_slot_id ~ ChooseUniformly(possibilities[:time_slot_id])
        day ~ ChooseUniformly(possibilities[:day])
        start_hour ~ ChooseUniformly(possibilities[:start_hour])
        start_minute ~ ChooseUniformly(possibilities[:start_minute])
        end_hour ~ ChooseUniformly(possibilities[:end_hour])
        end_minute ~ ChooseUniformly(possibilities[:end_minute])
    end

    @class Prerequisite begin
        course ~ Course
    end

    @class Obs begin
        teaches ~ Teaches
        takes_classes ~ Takes_classes
        advisor ~ Advisor
        time_slot ~ Time_slot
        prerequisite ~ Prerequisite
    end
end

query = @query College2Model.Obs [
    classroom_building teaches.section.classroom.building
    classroom_room_number teaches.section.classroom.room_number
    classroom_capacity teaches.section.classroom.capacity
    department_name prerequisite.course.department.department_name
    department_building prerequisite.course.department.building
    department_budget prerequisite.course.department.budget
    course_id prerequisite.course.course_id
    course_title prerequisite.course.title
    course_credits prerequisite.course.credits
    instructor_id teaches.instructor.id
    instructor_name teaches.instructor.name
    instructor_salary teaches.instructor.salary
    section_id teaches.section.section_id
    section_semester teaches.section.semester
    section_year teaches.section.year
    section_time_slot_id teaches.section.time_slot_id
    student_id takes_classes.student.id
    student_name takes_classes.student.name
    student_total_credits takes_classes.student.total_credits
    takes_classes_grade takes_classes.grade
    time_slot_id time_slot.time_slot_id
    time_slot_day time_slot.day
    time_slot_start_hour time_slot.start_hour
    time_slot_start_minute time_slot.start_minute
    time_slot_end_hour time_slot.end_hour
    time_slot_end_minute time_slot.end_minute
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
