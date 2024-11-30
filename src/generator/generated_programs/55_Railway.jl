using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("railway_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("railway_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "railway id"], Any[0, "railway"], Any[0, "builder"], Any[0, "built"], Any[0, "wheels"], Any[0, "location"], Any[0, "objectnumber"], Any[1, "train id"], Any[1, "train num"], Any[1, "name"], Any[1, "from"], Any[1, "arrival"], Any[1, "railway id"], Any[2, "manager id"], Any[2, "name"], Any[2, "country"], Any[2, "working year starts"], Any[2, "age"], Any[2, "level"], Any[3, "railway id"], Any[3, "manager id"], Any[3, "from year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "railway id"], Any[0, "railway"], Any[0, "builder"], Any[0, "built"], Any[0, "wheels"], Any[0, "location"], Any[0, "objectnumber"], Any[1, "train id"], Any[1, "train num"], Any[1, "name"], Any[1, "from"], Any[1, "arrival"], Any[1, "railway id"], Any[2, "manager id"], Any[2, "name"], Any[2, "country"], Any[2, "working year starts"], Any[2, "age"], Any[2, "level"], Any[3, "railway id"], Any[3, "manager id"], Any[3, "from year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["railway id", "railway id", "manager id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "railway"], Any[0, "builder"], Any[0, "built"], Any[0, "wheels"], Any[0, "location"], Any[0, "objectnumber"], Any[1, "train id"], Any[1, "train num"], Any[1, "name"], Any[1, "from"], Any[1, "arrival"], Any[2, "name"], Any[2, "country"], Any[2, "working year starts"], Any[2, "age"], Any[2, "level"], Any[3, "from year"]]
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





PClean.@model RailwayModel begin
    @class Railway begin
        railway_id ~ Unmodeled()
        railway ~ ChooseUniformly(possibilities[:railway])
        builder ~ ChooseUniformly(possibilities[:builder])
        built ~ ChooseUniformly(possibilities[:built])
        wheels ~ ChooseUniformly(possibilities[:wheels])
        location ~ ChooseUniformly(possibilities[:location])
        objectnumber ~ ChooseUniformly(possibilities[:objectnumber])
    end

    @class Manager begin
        manager_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        working_year_starts ~ ChooseUniformly(possibilities[:working_year_starts])
        age ~ ChooseUniformly(possibilities[:age])
        level ~ ChooseUniformly(possibilities[:level])
    end

    @class Obs begin
        railway ~ Railway
        manager ~ Manager
        train_id ~ Unmodeled()
        train_num ~ ChooseUniformly(possibilities[:train_num])
        name ~ ChooseUniformly(possibilities[:name])
        from ~ ChooseUniformly(possibilities[:from])
        arrival ~ ChooseUniformly(possibilities[:arrival])
        from_year ~ ChooseUniformly(possibilities[:from_year])
    end
end

query = @query RailwayModel.Obs [
    railway_id railway.railway_id
    railway railway.railway
    railway_builder railway.builder
    railway_built railway.built
    railway_wheels railway.wheels
    railway_location railway.location
    railway_objectnumber railway.objectnumber
    train_id train_id
    train_num train_num
    train_name name
    train_from from
    train_arrival arrival
    manager_id manager.manager_id
    manager_name manager.name
    manager_country manager.country
    manager_working_year_starts manager.working_year_starts
    manager_age manager.age
    manager_level manager.level
    railway_manage_from_year from_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
