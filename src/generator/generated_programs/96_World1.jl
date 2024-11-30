using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("city_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("city_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "country code"], Any[0, "district"], Any[0, "population"], Any[1, "name"], Any[1, "seq"], Any[2, "code"], Any[2, "name"], Any[2, "continent"], Any[2, "region"], Any[2, "surface area"], Any[2, "indepdent year"], Any[2, "population"], Any[2, "life expectancy"], Any[2, "gnp"], Any[2, "gnp old"], Any[2, "local name"], Any[2, "government form"], Any[2, "head of state"], Any[2, "capital"], Any[2, "code2"], Any[3, "countrycode"], Any[3, "language"], Any[3, "is official"], Any[3, "percentage"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "country code"], Any[0, "district"], Any[0, "population"], Any[1, "name"], Any[1, "seq"], Any[2, "code"], Any[2, "name"], Any[2, "continent"], Any[2, "region"], Any[2, "surface area"], Any[2, "indepdent year"], Any[2, "population"], Any[2, "life expectancy"], Any[2, "gnp"], Any[2, "gnp old"], Any[2, "local name"], Any[2, "government form"], Any[2, "head of state"], Any[2, "capital"], Any[2, "code2"], Any[3, "countrycode"], Any[3, "language"], Any[3, "is official"], Any[3, "percentage"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["country code", "countrycode"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "district"], Any[0, "population"], Any[1, "name"], Any[1, "seq"], Any[2, "code"], Any[2, "name"], Any[2, "continent"], Any[2, "region"], Any[2, "surface area"], Any[2, "indepdent year"], Any[2, "population"], Any[2, "life expectancy"], Any[2, "gnp"], Any[2, "gnp old"], Any[2, "local name"], Any[2, "government form"], Any[2, "head of state"], Any[2, "capital"], Any[2, "code2"], Any[3, "language"], Any[3, "is official"], Any[3, "percentage"]]
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





PClean.@model World1Model begin
    @class Sqlite_Sequence begin
        name ~ ChooseUniformly(possibilities[:name])
        seq ~ ChooseUniformly(possibilities[:seq])
    end

    @class Country begin
        code ~ ChooseUniformly(possibilities[:code])
        name ~ ChooseUniformly(possibilities[:name])
        continent ~ ChooseUniformly(possibilities[:continent])
        region ~ ChooseUniformly(possibilities[:region])
        surface_area ~ ChooseUniformly(possibilities[:surface_area])
        indepdent_year ~ ChooseUniformly(possibilities[:indepdent_year])
        population ~ ChooseUniformly(possibilities[:population])
        life_expectancy ~ ChooseUniformly(possibilities[:life_expectancy])
        gnp ~ ChooseUniformly(possibilities[:gnp])
        gnp_old ~ ChooseUniformly(possibilities[:gnp_old])
        local_name ~ ChooseUniformly(possibilities[:local_name])
        government_form ~ ChooseUniformly(possibilities[:government_form])
        head_of_state ~ ChooseUniformly(possibilities[:head_of_state])
        capital ~ ChooseUniformly(possibilities[:capital])
        code2 ~ ChooseUniformly(possibilities[:code2])
    end

    @class Obs begin
        sqlite_Sequence ~ Sqlite_Sequence
        country ~ Country
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        district ~ ChooseUniformly(possibilities[:district])
        population ~ ChooseUniformly(possibilities[:population])
        language ~ ChooseUniformly(possibilities[:language])
        is_official ~ ChooseUniformly(possibilities[:is_official])
        percentage ~ ChooseUniformly(possibilities[:percentage])
    end
end

query = @query World1Model.Obs [
    city_id id
    city_name name
    city_district district
    city_population population
    sqlite_sequence_name sqlite_Sequence.name
    sqlite_sequence_seq sqlite_Sequence.seq
    country_code country.code
    country_name country.name
    country_continent country.continent
    country_region country.region
    country_surface_area country.surface_area
    country_indepdent_year country.indepdent_year
    country_population country.population
    country_life_expectancy country.life_expectancy
    country_gnp country.gnp
    country_gnp_old country.gnp_old
    country_local_name country.local_name
    country_government_form country.government_form
    country_head_of_state country.head_of_state
    country_capital country.capital
    country_code2 country.code2
    countrylanguage_language language
    countrylanguage_is_official is_official
    countrylanguage_percentage percentage
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
