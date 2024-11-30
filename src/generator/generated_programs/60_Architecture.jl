using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("architect_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("architect_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["architect id", "architect id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "nationality"], Any[0, "gender"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "length meters"], Any[1, "length feet"], Any[2, "id"], Any[2, "location"], Any[2, "name"], Any[2, "type"], Any[2, "built year"], Any[2, "notes"]]
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





PClean.@model ArchitectureModel begin
    @class Architect begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        gender ~ ChooseUniformly(possibilities[:gender])
    end

    @class Obs begin
        architect ~ Architect
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        length_meters ~ ChooseUniformly(possibilities[:length_meters])
        length_feet ~ ChooseUniformly(possibilities[:length_feet])
        id ~ ChooseUniformly(possibilities[:id])
        location ~ ChooseUniformly(possibilities[:location])
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        built_year ~ ChooseUniformly(possibilities[:built_year])
        notes ~ ChooseUniformly(possibilities[:notes])
    end
end

query = @query ArchitectureModel.Obs [
    architect_id architect.id
    architect_name architect.name
    architect_nationality architect.nationality
    architect_gender architect.gender
    bridge_id id
    bridge_name name
    bridge_location location
    bridge_length_meters length_meters
    bridge_length_feet length_feet
    mill_id id
    mill_location location
    mill_name name
    mill_type type
    mill_built_year built_year
    mill_notes notes
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
