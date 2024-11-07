using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("entrepreneur_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("entrepreneur_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "entrepreneur id"], Any[0, "people id"], Any[0, "company"], Any[0, "money requested"], Any[0, "investor"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "date of birth"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "entrepreneur id"], Any[0, "people id"], Any[0, "company"], Any[0, "money requested"], Any[0, "investor"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "date of birth"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model EntrepreneurModel begin
    @class Entrepreneur begin
        entrepreneur_id ~ Unmodeled()
        people_id ~ ChooseUniformly(possibilities[:people_id])
        company ~ ChooseUniformly(possibilities[:company])
        money_requested ~ ChooseUniformly(possibilities[:money_requested])
        investor ~ ChooseUniformly(possibilities[:investor])
    end

    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
        date_of_birth ~ ChooseUniformly(possibilities[:date_of_birth])
    end

    @class Obs begin
        entrepreneur ~ Entrepreneur
        people ~ People
    end
end

query = @query EntrepreneurModel.Obs [
    entrepreneur_id entrepreneur.entrepreneur_id
    entrepreneur_company entrepreneur.company
    entrepreneur_money_requested entrepreneur.money_requested
    entrepreneur_investor entrepreneur.investor
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

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
