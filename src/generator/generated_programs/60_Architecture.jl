using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("architect_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("architect_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "architect id"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "architect id"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "architect id"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "architect id"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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
        architect_id ~ Unmodeled()
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        length_meters ~ ChooseUniformly(possibilities[:length_meters])
        length_feet ~ ChooseUniformly(possibilities[:length_feet])
    end

    @class Mill begin
        architect_id ~ Unmodeled()
        id ~ ChooseUniformly(possibilities[:id])
        location ~ ChooseUniformly(possibilities[:location])
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        built_year ~ ChooseUniformly(possibilities[:built_year])
        notes ~ ChooseUniformly(possibilities[:notes])
    end

    @class Obs begin
        architect ~ Architect
        bridge ~ Bridge
        mill ~ Mill
    end
end

query = @query ArchitectureModel.Obs [
    architect_id architect.id
    architect_name architect.name
    architect_nationality architect.nationality
    architect_gender architect.gender
    bridge_id bridge.id
    bridge_name bridge.name
    bridge_location bridge.location
    bridge_length_meters bridge.length_meters
    bridge_length_feet bridge.length_feet
    mill_id mill.id
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
