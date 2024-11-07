using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("classroom_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("classroom_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "building"], Any[0, "room number"], Any[0, "capacity"], Any[1, "department name"], Any[1, "building"], Any[1, "budget"], Any[2, "course id"], Any[2, "title"], Any[2, "department name"], Any[2, "credits"], Any[3, "id"], Any[3, "name"], Any[3, "department name"], Any[3, "salary"], Any[4, "course id"], Any[4, "section id"], Any[4, "semester"], Any[4, "year"], Any[4, "building"], Any[4, "room number"], Any[4, "time slot id"], Any[5, "id"], Any[5, "course id"], Any[5, "section id"], Any[5, "semester"], Any[5, "year"], Any[6, "id"], Any[6, "name"], Any[6, "department name"], Any[6, "total credits"], Any[7, "id"], Any[7, "course id"], Any[7, "section id"], Any[7, "semester"], Any[7, "year"], Any[7, "grade"], Any[8, "student id"], Any[8, "instructor id"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"], Any[10, "course id"], Any[10, "prerequisite id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "building"], Any[0, "room number"], Any[0, "capacity"], Any[1, "department name"], Any[1, "building"], Any[1, "budget"], Any[2, "course id"], Any[2, "title"], Any[2, "department name"], Any[2, "credits"], Any[3, "id"], Any[3, "name"], Any[3, "department name"], Any[3, "salary"], Any[4, "course id"], Any[4, "section id"], Any[4, "semester"], Any[4, "year"], Any[4, "building"], Any[4, "room number"], Any[4, "time slot id"], Any[5, "id"], Any[5, "course id"], Any[5, "section id"], Any[5, "semester"], Any[5, "year"], Any[6, "id"], Any[6, "name"], Any[6, "department name"], Any[6, "total credits"], Any[7, "id"], Any[7, "course id"], Any[7, "section id"], Any[7, "semester"], Any[7, "year"], Any[7, "grade"], Any[8, "student id"], Any[8, "instructor id"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"], Any[10, "course id"], Any[10, "prerequisite id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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
        department_name ~ ChooseUniformly(possibilities[:department_name])
        credits ~ ChooseUniformly(possibilities[:credits])
    end

    @class Instructor begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        department_name ~ ChooseUniformly(possibilities[:department_name])
        salary ~ ChooseUniformly(possibilities[:salary])
    end

    @class Section begin
        course_id ~ ChooseUniformly(possibilities[:course_id])
        section_id ~ ChooseUniformly(possibilities[:section_id])
        semester ~ ChooseUniformly(possibilities[:semester])
        year ~ ChooseUniformly(possibilities[:year])
        building ~ ChooseUniformly(possibilities[:building])
        room_number ~ ChooseUniformly(possibilities[:room_number])
        time_slot_id ~ ChooseUniformly(possibilities[:time_slot_id])
    end

    @class Teaches begin
        id ~ ChooseUniformly(possibilities[:id])
        course_id ~ ChooseUniformly(possibilities[:course_id])
        section_id ~ ChooseUniformly(possibilities[:section_id])
        semester ~ ChooseUniformly(possibilities[:semester])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Student begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        department_name ~ ChooseUniformly(possibilities[:department_name])
        total_credits ~ ChooseUniformly(possibilities[:total_credits])
    end

    @class Takes_Classes begin
        id ~ ChooseUniformly(possibilities[:id])
        course_id ~ ChooseUniformly(possibilities[:course_id])
        section_id ~ ChooseUniformly(possibilities[:section_id])
        semester ~ ChooseUniformly(possibilities[:semester])
        year ~ ChooseUniformly(possibilities[:year])
        grade ~ ChooseUniformly(possibilities[:grade])
    end

    @class Advisor begin
        student_id ~ ChooseUniformly(possibilities[:student_id])
        instructor_id ~ ChooseUniformly(possibilities[:instructor_id])
    end

    @class Time_Slot begin
        time_slot_id ~ ChooseUniformly(possibilities[:time_slot_id])
        day ~ ChooseUniformly(possibilities[:day])
        start_hour ~ ChooseUniformly(possibilities[:start_hour])
        start_minute ~ ChooseUniformly(possibilities[:start_minute])
        end_hour ~ ChooseUniformly(possibilities[:end_hour])
        end_minute ~ ChooseUniformly(possibilities[:end_minute])
    end

    @class Prerequisite begin
        course_id ~ ChooseUniformly(possibilities[:course_id])
        prerequisite_id ~ ChooseUniformly(possibilities[:prerequisite_id])
    end

    @class Obs begin
        classroom ~ Classroom
        department ~ Department
        course ~ Course
        instructor ~ Instructor
        section ~ Section
        teaches ~ Teaches
        student ~ Student
        takes_Classes ~ Takes_Classes
        advisor ~ Advisor
        time_Slot ~ Time_Slot
        prerequisite ~ Prerequisite
    end
end

query = @query College2Model.Obs [
    classroom_building classroom.building
    classroom_room_number classroom.room_number
    classroom_capacity classroom.capacity
    department_name department.department_name
    department_building department.building
    department_budget department.budget
    course_id course.course_id
    course_title course.title
    course_credits course.credits
    instructor_id instructor.id
    instructor_name instructor.name
    instructor_salary instructor.salary
    section_id section.section_id
    section_semester section.semester
    section_year section.year
    section_time_slot_id section.time_slot_id
    student_id student.id
    student_name student.name
    student_total_credits student.total_credits
    takes_classes_grade takes_Classes.grade
    time_slot_id time_Slot.time_slot_id
    time_slot_day time_Slot.day
    time_slot_start_hour time_Slot.start_hour
    time_slot_start_minute time_Slot.start_minute
    time_slot_end_hour time_Slot.end_hour
    time_slot_end_minute time_Slot.end_minute
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
