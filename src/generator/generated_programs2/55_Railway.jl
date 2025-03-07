using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("railway_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("railway_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "railway id"], Any[0, "railway"], Any[0, "builder"], Any[0, "built"], Any[0, "wheels"], Any[0, "location"], Any[0, "objectnumber"], Any[1, "train id"], Any[1, "train num"], Any[1, "name"], Any[1, "from"], Any[1, "arrival"], Any[1, "railway id"], Any[2, "manager id"], Any[2, "name"], Any[2, "country"], Any[2, "working year starts"], Any[2, "age"], Any[2, "level"], Any[3, "railway id"], Any[3, "manager id"], Any[3, "from year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 1], Any[20, 1], Any[21, 14]])
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







PClean.@model RailwayModel begin
    @class Railway begin
        railway ~ ChooseUniformly(possibilities[:railway])
        builder ~ ChooseUniformly(possibilities[:builder])
        built ~ ChooseUniformly(possibilities[:built])
        wheels ~ ChooseUniformly(possibilities[:wheels])
        location ~ ChooseUniformly(possibilities[:location])
        objectnumber ~ ChooseUniformly(possibilities[:objectnumber])
    end

    @class Train begin
        train_num ~ ChooseUniformly(possibilities[:train_num])
        name ~ ChooseUniformly(possibilities[:name])
        from ~ ChooseUniformly(possibilities[:from])
        arrival ~ ChooseUniformly(possibilities[:arrival])
        railway ~ Railway
    end

    @class Manager begin
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        working_year_starts ~ ChooseUniformly(possibilities[:working_year_starts])
        age ~ ChooseUniformly(possibilities[:age])
        level ~ ChooseUniformly(possibilities[:level])
    end

    @class Railway_manage begin
        manager ~ Manager
        from_year ~ ChooseUniformly(possibilities[:from_year])
    end

    @class Obs begin
        train ~ Train
        railway_manage ~ Railway_manage
    end
end

query = @query RailwayModel.Obs [
    railway_id train.railway.railway_id
    railway train.railway.railway
    railway_builder train.railway.builder
    railway_built train.railway.built
    railway_wheels train.railway.wheels
    railway_location train.railway.location
    railway_objectnumber train.railway.objectnumber
    train_id train.train_id
    train_num train.train_num
    train_name train.name
    train_from train.from
    train_arrival train.arrival
    manager_id railway_manage.manager.manager_id
    manager_name railway_manage.manager.name
    manager_country railway_manage.manager.country
    manager_working_year_starts railway_manage.manager.working_year_starts
    manager_age railway_manage.manager.age
    manager_level railway_manage.manager.level
    railway_manage_from_year railway_manage.from_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
