using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("roller_coaster_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("roller_coaster_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "roller coaster id"], Any[0, "name"], Any[0, "park"], Any[0, "country id"], Any[0, "length"], Any[0, "height"], Any[0, "speed"], Any[0, "opened"], Any[0, "status"], Any[1, "country id"], Any[1, "name"], Any[1, "population"], Any[1, "area"], Any[1, "languages"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[4, 10]])
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







PClean.@model RollerCoasterModel begin
    @class Country begin
        name ~ ChooseUniformly(possibilities[:name])
        population ~ ChooseUniformly(possibilities[:population])
        area ~ ChooseUniformly(possibilities[:area])
        languages ~ ChooseUniformly(possibilities[:languages])
    end

    @class Roller_coaster begin
        name ~ ChooseUniformly(possibilities[:name])
        park ~ ChooseUniformly(possibilities[:park])
        country ~ Country
        length ~ ChooseUniformly(possibilities[:length])
        height ~ ChooseUniformly(possibilities[:height])
        speed ~ ChooseUniformly(possibilities[:speed])
        opened ~ ChooseUniformly(possibilities[:opened])
        status ~ ChooseUniformly(possibilities[:status])
    end

    @class Obs begin
        roller_coaster ~ Roller_coaster
    end
end

query = @query RollerCoasterModel.Obs [
    roller_coaster_id roller_coaster.roller_coaster_id
    roller_coaster_name roller_coaster.name
    roller_coaster_park roller_coaster.park
    roller_coaster_length roller_coaster.length
    roller_coaster_height roller_coaster.height
    roller_coaster_speed roller_coaster.speed
    roller_coaster_opened roller_coaster.opened
    roller_coaster_status roller_coaster.status
    country_id roller_coaster.country.country_id
    country_name roller_coaster.country.name
    country_population roller_coaster.country.population
    country_area roller_coaster.country.area
    country_languages roller_coaster.country.languages
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
