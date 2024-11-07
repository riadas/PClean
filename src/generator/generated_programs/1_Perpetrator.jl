using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("perpetrator_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("perpetrator_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "perpetrator id"], Any[0, "people id"], Any[0, "date"], Any[0, "year"], Any[0, "location"], Any[0, "country"], Any[0, "killed"], Any[0, "injured"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "home town"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "perpetrator id"], Any[0, "people id"], Any[0, "date"], Any[0, "year"], Any[0, "location"], Any[0, "country"], Any[0, "killed"], Any[0, "injured"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "home town"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model PerpetratorModel begin
    @class Perpetrator begin
        perpetrator_id ~ Unmodeled()
        people_id ~ ChooseUniformly(possibilities[:people_id])
        date ~ ChooseUniformly(possibilities[:date])
        year ~ ChooseUniformly(possibilities[:year])
        location ~ ChooseUniformly(possibilities[:location])
        country ~ ChooseUniformly(possibilities[:country])
        killed ~ ChooseUniformly(possibilities[:killed])
        injured ~ ChooseUniformly(possibilities[:injured])
    end

    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
        home_town ~ ChooseUniformly(possibilities[:home_town])
    end

    @class Obs begin
        perpetrator ~ Perpetrator
        people ~ People
    end
end

query = @query PerpetratorModel.Obs [
    perpetrator_id perpetrator.perpetrator_id
    perpetrator_date perpetrator.date
    perpetrator_year perpetrator.year
    perpetrator_location perpetrator.location
    perpetrator_country perpetrator.country
    perpetrator_killed perpetrator.killed
    perpetrator_injured perpetrator.injured
    people_id people.people_id
    people_name people.name
    people_height people.height
    people_weight people.weight
    people_home_town people.home_town
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
