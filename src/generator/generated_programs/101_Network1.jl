using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("high schooler_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("high schooler_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "grade"], Any[1, "student id"], Any[1, "friend id"], Any[2, "student id"], Any[2, "liked id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "grade"], Any[1, "student id"], Any[1, "friend id"], Any[2, "student id"], Any[2, "liked id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Network1Model begin
    @class High_Schooler begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        grade ~ ChooseUniformly(possibilities[:grade])
    end

    @class Friend begin
        student_id ~ Unmodeled()
        friend_id ~ ChooseUniformly(possibilities[:friend_id])
    end

    @class Likes begin
        student_id ~ Unmodeled()
        liked_id ~ ChooseUniformly(possibilities[:liked_id])
    end

    @class Obs begin
        high_Schooler ~ High_Schooler
        friend ~ Friend
        likes ~ Likes
    end
end

query = @query Network1Model.Obs [
    high_schooler_id high_Schooler.id
    high_schooler_name high_Schooler.name
    high_schooler_grade high_Schooler.grade
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
