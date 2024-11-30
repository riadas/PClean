using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("roller coaster_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("roller coaster_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "roller coaster id"], Any[0, "name"], Any[0, "park"], Any[0, "country id"], Any[0, "length"], Any[0, "height"], Any[0, "speed"], Any[0, "opened"], Any[0, "status"], Any[1, "country id"], Any[1, "name"], Any[1, "population"], Any[1, "area"], Any[1, "languages"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "roller coaster id"], Any[0, "name"], Any[0, "park"], Any[0, "country id"], Any[0, "length"], Any[0, "height"], Any[0, "speed"], Any[0, "opened"], Any[0, "status"], Any[1, "country id"], Any[1, "name"], Any[1, "population"], Any[1, "area"], Any[1, "languages"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["country id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "roller coaster id"], Any[0, "name"], Any[0, "park"], Any[0, "length"], Any[0, "height"], Any[0, "speed"], Any[0, "opened"], Any[0, "status"], Any[1, "name"], Any[1, "population"], Any[1, "area"], Any[1, "languages"]]
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





PClean.@model RollerCoasterModel begin
    @class Country begin
        country_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        population ~ ChooseUniformly(possibilities[:population])
        area ~ ChooseUniformly(possibilities[:area])
        languages ~ ChooseUniformly(possibilities[:languages])
    end

    @class Obs begin
        country ~ Country
        roller_coaster_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        park ~ ChooseUniformly(possibilities[:park])
        length ~ ChooseUniformly(possibilities[:length])
        height ~ ChooseUniformly(possibilities[:height])
        speed ~ ChooseUniformly(possibilities[:speed])
        opened ~ ChooseUniformly(possibilities[:opened])
        status ~ ChooseUniformly(possibilities[:status])
    end
end

query = @query RollerCoasterModel.Obs [
    roller_coaster_id roller_coaster_id
    roller_coaster_name name
    roller_coaster_park park
    roller_coaster_length length
    roller_coaster_height height
    roller_coaster_speed speed
    roller_coaster_opened opened
    roller_coaster_status status
    country_id country.country_id
    country_name country.name
    country_population country.population
    country_area country.area
    country_languages country.languages
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
