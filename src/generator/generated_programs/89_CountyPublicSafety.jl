using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("county public safety_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("county public safety_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["county id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "population"], Any[0, "police officers"], Any[0, "residents per officer"], Any[0, "case burden"], Any[0, "crime rate"], Any[0, "police force"], Any[0, "location"], Any[1, "city id"], Any[1, "name"], Any[1, "white"], Any[1, "black"], Any[1, "amerindian"], Any[1, "asian"], Any[1, "multiracial"], Any[1, "hispanic"]]
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

    @class Obs begin
        county_Public_Safety ~ County_Public_Safety
        city_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        white ~ ChooseUniformly(possibilities[:white])
        black ~ ChooseUniformly(possibilities[:black])
        amerindian ~ ChooseUniformly(possibilities[:amerindian])
        asian ~ ChooseUniformly(possibilities[:asian])
        multiracial ~ ChooseUniformly(possibilities[:multiracial])
        hispanic ~ ChooseUniformly(possibilities[:hispanic])
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
    city_id city_id
    city_name name
    city_white white
    city_black black
    city_amerindian amerindian
    city_asian asian
    city_multiracial multiracial
    city_hispanic hispanic
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
