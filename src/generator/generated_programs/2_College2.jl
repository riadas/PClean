using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("classroom_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("classroom_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["department name", "department name", "building", "room number", "course id", "id", "course id", "section id", "semester", "year", "department name", "id", "course id", "section id", "semester", "year", "student id", "instructor id", "prerequisite id", "course id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "capacity"], Any[1, "budget"], Any[2, "title"], Any[2, "credits"], Any[3, "name"], Any[3, "salary"], Any[4, "time slot id"], Any[6, "name"], Any[6, "total credits"], Any[7, "grade"], Any[9, "time slot id"], Any[9, "day"], Any[9, "start hour"], Any[9, "start minute"], Any[9, "end hour"], Any[9, "end minute"]]
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

    @class Time_Slot begin
        time_slot_id ~ ChooseUniformly(possibilities[:time_slot_id])
        day ~ ChooseUniformly(possibilities[:day])
        start_hour ~ ChooseUniformly(possibilities[:start_hour])
        start_minute ~ ChooseUniformly(possibilities[:start_minute])
        end_hour ~ ChooseUniformly(possibilities[:end_hour])
        end_minute ~ ChooseUniformly(possibilities[:end_minute])
    end

    @class Obs begin
        classroom ~ Classroom
        department ~ Department
        time_Slot ~ Time_Slot
        course_id ~ ChooseUniformly(possibilities[:course_id])
        title ~ ChooseUniformly(possibilities[:title])
        credits ~ ChooseUniformly(possibilities[:credits])
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        salary ~ ChooseUniformly(possibilities[:salary])
        section_id ~ ChooseUniformly(possibilities[:section_id])
        semester ~ ChooseUniformly(possibilities[:semester])
        year ~ ChooseUniformly(possibilities[:year])
        time_slot_id ~ ChooseUniformly(possibilities[:time_slot_id])
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        total_credits ~ ChooseUniformly(possibilities[:total_credits])
        grade ~ ChooseUniformly(possibilities[:grade])
    end
end

query = @query College2Model.Obs [
    classroom_building classroom.building
    classroom_room_number classroom.room_number
    classroom_capacity classroom.capacity
    department_name department.department_name
    department_building department.building
    department_budget department.budget
    course_id course_id
    course_title title
    course_credits credits
    instructor_id id
    instructor_name name
    instructor_salary salary
    section_id section_id
    section_semester semester
    section_year year
    section_time_slot_id time_slot_id
    student_id id
    student_name name
    student_total_credits total_credits
    takes_classes_grade grade
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

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
