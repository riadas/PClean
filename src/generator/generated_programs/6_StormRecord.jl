using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("storm_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("storm_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "storm id"], Any[0, "name"], Any[0, "dates active"], Any[0, "max speed"], Any[0, "damage millions usd"], Any[0, "number deaths"], Any[1, "region id"], Any[1, "region code"], Any[1, "region name"], Any[2, "region id"], Any[2, "storm id"], Any[2, "number city affected"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "storm id"], Any[0, "name"], Any[0, "dates active"], Any[0, "max speed"], Any[0, "damage millions usd"], Any[0, "number deaths"], Any[1, "region id"], Any[1, "region code"], Any[1, "region name"], Any[2, "region id"], Any[2, "storm id"], Any[2, "number city affected"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model StormRecordModel begin
    @class Storm begin
        storm_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        dates_active ~ ChooseUniformly(possibilities[:dates_active])
        max_speed ~ ChooseUniformly(possibilities[:max_speed])
        damage_millions_usd ~ ChooseUniformly(possibilities[:damage_millions_usd])
        number_deaths ~ ChooseUniformly(possibilities[:number_deaths])
    end

    @class Region begin
        region_id ~ Unmodeled()
        region_code ~ ChooseUniformly(possibilities[:region_code])
        region_name ~ ChooseUniformly(possibilities[:region_name])
    end

    @class Affected_Region begin
        region_id ~ Unmodeled()
        storm_id ~ ChooseUniformly(possibilities[:storm_id])
        number_city_affected ~ ChooseUniformly(possibilities[:number_city_affected])
    end

    @class Obs begin
        storm ~ Storm
        region ~ Region
        affected_Region ~ Affected_Region
    end
end

query = @query StormRecordModel.Obs [
    storm_id storm.storm_id
    storm_name storm.name
    storm_dates_active storm.dates_active
    storm_max_speed storm.max_speed
    storm_damage_millions_usd storm.damage_millions_usd
    storm_number_deaths storm.number_deaths
    region_id region.region_id
    region_code region.region_code
    region_name region.region_name
    affected_region_number_city_affected affected_Region.number_city_affected
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
