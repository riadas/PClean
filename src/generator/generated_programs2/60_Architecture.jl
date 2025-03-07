using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("architect_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("architect_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "architect id"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "architect id"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "architect id"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "architect id"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "architect id"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "architect id"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "architect id"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "architect id"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "architect id"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "architect id"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 1], Any[11, 1]])
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







PClean.@model ArchitectureModel begin
    @class Architect begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        gender ~ ChooseUniformly(possibilities[:gender])
    end

    @class Bridge begin
        architect ~ Architect
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        length_meters ~ ChooseUniformly(possibilities[:length_meters])
        length_feet ~ ChooseUniformly(possibilities[:length_feet])
    end

    @class Mill begin
        architect ~ Architect
        location ~ ChooseUniformly(possibilities[:location])
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        built_year ~ ChooseUniformly(possibilities[:built_year])
        notes ~ ChooseUniformly(possibilities[:notes])
    end

    @class Obs begin
        bridge ~ Bridge
        mill ~ Mill
    end
end

query = @query ArchitectureModel.Obs [
    architect_id bridge.architect.id
    architect_name bridge.architect.name
    architect_nationality bridge.architect.nationality
    architect_gender bridge.architect.gender
    bridge_name bridge.name
    bridge_location bridge.location
    bridge_length_meters bridge.length_meters
    bridge_length_feet bridge.length_feet
    mill_location mill.location
    mill_name mill.name
    mill_type mill.type
    mill_built_year mill.built_year
    mill_notes mill.notes
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
