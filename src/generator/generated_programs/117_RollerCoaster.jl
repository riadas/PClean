using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("roller coaster_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("roller coaster_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "roller coaster id"], Any[0, "name"], Any[0, "park"], Any[0, "country id"], Any[0, "length"], Any[0, "height"], Any[0, "speed"], Any[0, "opened"], Any[0, "status"], Any[1, "country id"], Any[1, "name"], Any[1, "population"], Any[1, "area"], Any[1, "languages"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "roller coaster id"], Any[0, "name"], Any[0, "park"], Any[0, "country id"], Any[0, "length"], Any[0, "height"], Any[0, "speed"], Any[0, "opened"], Any[0, "status"], Any[1, "country id"], Any[1, "name"], Any[1, "population"], Any[1, "area"], Any[1, "languages"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model RollerCoasterModel begin
    @class Roller_Coaster begin
        roller_coaster_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        park ~ ChooseUniformly(possibilities[:park])
        country_id ~ ChooseUniformly(possibilities[:country_id])
        length ~ ChooseUniformly(possibilities[:length])
        height ~ ChooseUniformly(possibilities[:height])
        speed ~ ChooseUniformly(possibilities[:speed])
        opened ~ ChooseUniformly(possibilities[:opened])
        status ~ ChooseUniformly(possibilities[:status])
    end

    @class Country begin
        country_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        population ~ ChooseUniformly(possibilities[:population])
        area ~ ChooseUniformly(possibilities[:area])
        languages ~ ChooseUniformly(possibilities[:languages])
    end

    @class Obs begin
        roller_Coaster ~ Roller_Coaster
        country ~ Country
    end
end

query = @query RollerCoasterModel.Obs [
    roller_coaster_id roller_Coaster.roller_coaster_id
    roller_coaster_name roller_Coaster.name
    roller_coaster_park roller_Coaster.park
    roller_coaster_length roller_Coaster.length
    roller_coaster_height roller_Coaster.height
    roller_coaster_speed roller_Coaster.speed
    roller_coaster_opened roller_Coaster.opened
    roller_coaster_status roller_Coaster.status
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

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
