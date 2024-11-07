using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("list_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("list_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "last name"], Any[0, "first name"], Any[0, "grade"], Any[0, "class room"], Any[1, "last name"], Any[1, "first name"], Any[1, "class room"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "last name"], Any[0, "first name"], Any[0, "grade"], Any[0, "class room"], Any[1, "last name"], Any[1, "first name"], Any[1, "class room"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Student1Model begin
    @class List begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        grade ~ ChooseUniformly(possibilities[:grade])
        class_room ~ ChooseUniformly(possibilities[:class_room])
    end

    @class Teachers begin
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        class_room ~ ChooseUniformly(possibilities[:class_room])
    end

    @class Obs begin
        list ~ List
        teachers ~ Teachers
    end
end

query = @query Student1Model.Obs [
    list_last_name list.last_name
    list_first_name list.first_name
    list_grade list.grade
    list_class_room list.class_room
    teachers_last_name teachers.last_name
    teachers_first_name teachers.first_name
    teachers_class_room teachers.class_room
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
