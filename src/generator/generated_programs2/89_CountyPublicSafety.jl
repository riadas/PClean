using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("county_public_safety_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("county_public_safety_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "county id"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "county id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "county id"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "county id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "county id"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "county id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "county id"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "county id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "county id"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "county id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 1]])
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







PClean.@model CountyPublicSafetyModel begin
    @class County_public_safety begin
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
        county_public_safety ~ County_public_safety
        name ~ ChooseUniformly(possibilities[:name])
        white ~ ChooseUniformly(possibilities[:white])
        black ~ ChooseUniformly(possibilities[:black])
        amerindian ~ ChooseUniformly(possibilities[:amerindian])
        asian ~ ChooseUniformly(possibilities[:asian])
        multiracial ~ ChooseUniformly(possibilities[:multiracial])
        hispanic ~ ChooseUniformly(possibilities[:hispanic])
    end

    @class Obs begin
        city ~ City
    end
end

query = @query CountyPublicSafetyModel.Obs [
    county_public_safety_county_id city.county_public_safety.county_id
    county_public_safety_name city.county_public_safety.name
    county_public_safety_population city.county_public_safety.population
    county_public_safety_police_officers city.county_public_safety.police_officers
    county_public_safety_residents_per_officer city.county_public_safety.residents_per_officer
    county_public_safety_case_burden city.county_public_safety.case_burden
    county_public_safety_crime_rate city.county_public_safety.crime_rate
    county_public_safety_police_force city.county_public_safety.police_force
    county_public_safety_location city.county_public_safety.location
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
