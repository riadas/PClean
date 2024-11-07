using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("county public safety_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("county public safety_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "county id"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "county id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "county id"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "county id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CountyPublicSafetyModel begin
    @class County_Public_Safety begin
        county_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        population ~ ChooseUniformly(possibilities[:population])
        police_officers ~ ChooseUniformly(possibilities[:police_officers])
        residents_per_officer ~ ChooseUniformly(possibilities[:residents_per_officer])
        case_burden ~ ChooseUniformly(possibilities[:case_burden])
        crime_rate ~ ChooseUniformly(possibilities[:crime_rate])
        police_force ~ ChooseUniformly(possibilities[:police_force])
        location ~ ChooseUniformly(possibilities[:location])
    end

    @class City begin
        city_id ~ Unmodeled()
        county_id ~ ChooseUniformly(possibilities[:county_id])
        name ~ ChooseUniformly(possibilities[:name])
        white ~ ChooseUniformly(possibilities[:white])
        black ~ ChooseUniformly(possibilities[:black])
        amerindian ~ ChooseUniformly(possibilities[:amerindian])
        asian ~ ChooseUniformly(possibilities[:asian])
        multiracial ~ ChooseUniformly(possibilities[:multiracial])
        hispanic ~ ChooseUniformly(possibilities[:hispanic])
    end

    @class Obs begin
        county_Public_Safety ~ County_Public_Safety
        city ~ City
    end
end

query = @query CountyPublicSafetyModel.Obs [
    county_public_safety_county_id county_Public_Safety.county_id
    county_public_safety_name county_Public_Safety.name
    county_public_safety_population county_Public_Safety.population
    county_public_safety_police_officers county_Public_Safety.police_officers
    county_public_safety_residents_per_officer county_Public_Safety.residents_per_officer
    county_public_safety_case_burden county_Public_Safety.case_burden
    county_public_safety_crime_rate county_Public_Safety.crime_rate
    county_public_safety_police_force county_Public_Safety.police_force
    county_public_safety_location county_Public_Safety.location
    city_id city.city_id
    city_name city.name
    city_white city.white
    city_black city.black
    city_amerindian city.amerindian
    city_asian city.asian
    city_multiracial city.multiracial
    city_hispanic city.hispanic
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
