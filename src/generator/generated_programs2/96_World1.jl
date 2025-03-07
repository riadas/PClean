using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("city_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("city_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "country code"], Any[0, "district"], Any[0, "population"], Any[1, "name"], Any[1, "seq"], Any[2, "code"], Any[2, "name"], Any[2, "continent"], Any[2, "region"], Any[2, "surface area"], Any[2, "indepdent year"], Any[2, "population"], Any[2, "life expectancy"], Any[2, "gnp"], Any[2, "gnp old"], Any[2, "local name"], Any[2, "government form"], Any[2, "head of state"], Any[2, "capital"], Any[2, "code2"], Any[3, "countrycode"], Any[3, "language"], Any[3, "is official"], Any[3, "percentage"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[3, 8], Any[23, 8]])
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







PClean.@model World1Model begin
    @class Sqlite_sequence begin
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

    @class Countrylanguage begin
        country ~ Country
        language ~ ChooseUniformly(possibilities[:language])
        is_official ~ ChooseUniformly(possibilities[:is_official])
        percentage ~ ChooseUniformly(possibilities[:percentage])
    end

    @class City begin
        name ~ ChooseUniformly(possibilities[:name])
        country ~ Country
        district ~ ChooseUniformly(possibilities[:district])
        population ~ ChooseUniformly(possibilities[:population])
    end

    @class Obs begin
        city ~ City
        sqlite_sequence ~ Sqlite_sequence
        countrylanguage ~ Countrylanguage
    end
end

query = @query World1Model.Obs [
    city_name city.name
    city_district city.district
    city_population city.population
    sqlite_sequence_name sqlite_sequence.name
    sqlite_sequence_seq sqlite_sequence.seq
    country_code city.country.code
    country_name city.country.name
    country_continent city.country.continent
    country_region city.country.region
    country_surface_area city.country.surface_area
    country_indepdent_year city.country.indepdent_year
    country_population city.country.population
    country_life_expectancy city.country.life_expectancy
    country_gnp city.country.gnp
    country_gnp_old city.country.gnp_old
    country_local_name city.country.local_name
    country_government_form city.country.government_form
    country_head_of_state city.country.head_of_state
    country_capital city.country.capital
    country_code2 city.country.code2
    countrylanguage_language countrylanguage.language
    countrylanguage_is_official countrylanguage.is_official
    countrylanguage_percentage countrylanguage.percentage
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
