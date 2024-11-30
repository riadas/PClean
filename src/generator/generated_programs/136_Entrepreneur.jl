using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("entrepreneur_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("entrepreneur_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "entrepreneur id"], Any[0, "people id"], Any[0, "company"], Any[0, "money requested"], Any[0, "investor"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "date of birth"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "entrepreneur id"], Any[0, "people id"], Any[0, "company"], Any[0, "money requested"], Any[0, "investor"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "date of birth"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["people id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "entrepreneur id"], Any[0, "company"], Any[0, "money requested"], Any[0, "investor"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "date of birth"]]
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





PClean.@model EntrepreneurModel begin
    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
        date_of_birth ~ ChooseUniformly(possibilities[:date_of_birth])
    end

    @class Obs begin
        people ~ People
        entrepreneur_id ~ Unmodeled()
        company ~ ChooseUniformly(possibilities[:company])
        money_requested ~ ChooseUniformly(possibilities[:money_requested])
        investor ~ ChooseUniformly(possibilities[:investor])
    end
end

query = @query EntrepreneurModel.Obs [
    entrepreneur_id entrepreneur_id
    entrepreneur_company company
    entrepreneur_money_requested money_requested
    entrepreneur_investor investor
    people_id people.people_id
    people_name people.name
    people_height people.height
    people_weight people.weight
    people_date_of_birth people.date_of_birth
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
