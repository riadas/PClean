using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("perpetrator_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("perpetrator_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "perpetrator id"], Any[0, "people id"], Any[0, "date"], Any[0, "year"], Any[0, "location"], Any[0, "country"], Any[0, "killed"], Any[0, "injured"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "home town"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "perpetrator id"], Any[0, "people id"], Any[0, "date"], Any[0, "year"], Any[0, "location"], Any[0, "country"], Any[0, "killed"], Any[0, "injured"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "home town"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "perpetrator id"], Any[0, "people id"], Any[0, "date"], Any[0, "year"], Any[0, "location"], Any[0, "country"], Any[0, "killed"], Any[0, "injured"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "home town"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "perpetrator id"], Any[0, "people id"], Any[0, "date"], Any[0, "year"], Any[0, "location"], Any[0, "country"], Any[0, "killed"], Any[0, "injured"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "home town"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "perpetrator id"], Any[0, "people id"], Any[0, "date"], Any[0, "year"], Any[0, "location"], Any[0, "country"], Any[0, "killed"], Any[0, "injured"], Any[1, "people id"], Any[1, "name"], Any[1, "height"], Any[1, "weight"], Any[1, "home town"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[2, 9]])
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







PClean.@model PerpetratorModel begin
    @class People begin
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        weight ~ ChooseUniformly(possibilities[:weight])
        home_town ~ ChooseUniformly(possibilities[:home_town])
    end

    @class Perpetrator begin
        people ~ People
        date ~ ChooseUniformly(possibilities[:date])
        year ~ ChooseUniformly(possibilities[:year])
        location ~ ChooseUniformly(possibilities[:location])
        country ~ ChooseUniformly(possibilities[:country])
        killed ~ ChooseUniformly(possibilities[:killed])
        injured ~ ChooseUniformly(possibilities[:injured])
    end

    @class Obs begin
        perpetrator ~ Perpetrator
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
    people_id perpetrator.people.people_id
    people_name perpetrator.people.name
    people_height perpetrator.people.height
    people_weight perpetrator.people.weight
    people_home_town perpetrator.people.home_town
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
