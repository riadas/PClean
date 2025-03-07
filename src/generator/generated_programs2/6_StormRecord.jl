using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("storm_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("storm_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "storm id"], Any[0, "name"], Any[0, "dates active"], Any[0, "max speed"], Any[0, "damage millions usd"], Any[0, "number deaths"], Any[1, "region id"], Any[1, "region code"], Any[1, "region name"], Any[2, "region id"], Any[2, "storm id"], Any[2, "number city affected"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "storm id"], Any[0, "name"], Any[0, "dates active"], Any[0, "max speed"], Any[0, "damage millions usd"], Any[0, "number deaths"], Any[1, "region id"], Any[1, "region code"], Any[1, "region name"], Any[2, "region id"], Any[2, "storm id"], Any[2, "number city affected"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "storm id"], Any[0, "name"], Any[0, "dates active"], Any[0, "max speed"], Any[0, "damage millions usd"], Any[0, "number deaths"], Any[1, "region id"], Any[1, "region code"], Any[1, "region name"], Any[2, "region id"], Any[2, "storm id"], Any[2, "number city affected"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "storm id"], Any[0, "name"], Any[0, "dates active"], Any[0, "max speed"], Any[0, "damage millions usd"], Any[0, "number deaths"], Any[1, "region id"], Any[1, "region code"], Any[1, "region name"], Any[2, "region id"], Any[2, "storm id"], Any[2, "number city affected"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "storm id"], Any[0, "name"], Any[0, "dates active"], Any[0, "max speed"], Any[0, "damage millions usd"], Any[0, "number deaths"], Any[1, "region id"], Any[1, "region code"], Any[1, "region name"], Any[2, "region id"], Any[2, "storm id"], Any[2, "number city affected"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 1], Any[10, 7]])
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







PClean.@model StormRecordModel begin
    @class Storm begin
        name ~ ChooseUniformly(possibilities[:name])
        dates_active ~ ChooseUniformly(possibilities[:dates_active])
        max_speed ~ ChooseUniformly(possibilities[:max_speed])
        damage_millions_usd ~ ChooseUniformly(possibilities[:damage_millions_usd])
        number_deaths ~ ChooseUniformly(possibilities[:number_deaths])
    end

    @class Region begin
        region_code ~ ChooseUniformly(possibilities[:region_code])
        region_name ~ ChooseUniformly(possibilities[:region_name])
    end

    @class Affected_region begin
        storm ~ Storm
        number_city_affected ~ ChooseUniformly(possibilities[:number_city_affected])
    end

    @class Obs begin
        affected_region ~ Affected_region
    end
end

query = @query StormRecordModel.Obs [
    storm_id affected_region.storm.storm_id
    storm_name affected_region.storm.name
    storm_dates_active affected_region.storm.dates_active
    storm_max_speed affected_region.storm.max_speed
    storm_damage_millions_usd affected_region.storm.damage_millions_usd
    storm_number_deaths affected_region.storm.number_deaths
    region_id affected_region.region.region_id
    region_code affected_region.region.region_code
    region_name affected_region.region.region_name
    affected_region_number_city_affected affected_region.number_city_affected
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
