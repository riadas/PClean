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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "city id"], Any[0, "city"], Any[0, "hanzi"], Any[0, "hanyu pinyin"], Any[0, "regional population"], Any[0, "gdp"], Any[1, "match id"], Any[1, "date"], Any[1, "venue"], Any[1, "score"], Any[1, "result"], Any[1, "competition"], Any[2, "city id"], Any[2, "jan"], Any[2, "feb"], Any[2, "mar"], Any[2, "apr"], Any[2, "jun"], Any[2, "jul"], Any[2, "aug"], Any[2, "sep"], Any[2, "oct"], Any[2, "nov"], Any[2, "dec"], Any[3, "year"], Any[3, "match id"], Any[3, "host city"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "city id"], Any[0, "city"], Any[0, "hanzi"], Any[0, "hanyu pinyin"], Any[0, "regional population"], Any[0, "gdp"], Any[1, "match id"], Any[1, "date"], Any[1, "venue"], Any[1, "score"], Any[1, "result"], Any[1, "competition"], Any[2, "city id"], Any[2, "jan"], Any[2, "feb"], Any[2, "mar"], Any[2, "apr"], Any[2, "jun"], Any[2, "jul"], Any[2, "aug"], Any[2, "sep"], Any[2, "oct"], Any[2, "nov"], Any[2, "dec"], Any[3, "year"], Any[3, "match id"], Any[3, "host city"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "city id"], Any[0, "city"], Any[0, "hanzi"], Any[0, "hanyu pinyin"], Any[0, "regional population"], Any[0, "gdp"], Any[1, "match id"], Any[1, "date"], Any[1, "venue"], Any[1, "score"], Any[1, "result"], Any[1, "competition"], Any[2, "city id"], Any[2, "jan"], Any[2, "feb"], Any[2, "mar"], Any[2, "apr"], Any[2, "jun"], Any[2, "jul"], Any[2, "aug"], Any[2, "sep"], Any[2, "oct"], Any[2, "nov"], Any[2, "dec"], Any[3, "year"], Any[3, "match id"], Any[3, "host city"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "city id"], Any[0, "city"], Any[0, "hanzi"], Any[0, "hanyu pinyin"], Any[0, "regional population"], Any[0, "gdp"], Any[1, "match id"], Any[1, "date"], Any[1, "venue"], Any[1, "score"], Any[1, "result"], Any[1, "competition"], Any[2, "city id"], Any[2, "jan"], Any[2, "feb"], Any[2, "mar"], Any[2, "apr"], Any[2, "jun"], Any[2, "jul"], Any[2, "aug"], Any[2, "sep"], Any[2, "oct"], Any[2, "nov"], Any[2, "dec"], Any[3, "year"], Any[3, "match id"], Any[3, "host city"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "city id"], Any[0, "city"], Any[0, "hanzi"], Any[0, "hanyu pinyin"], Any[0, "regional population"], Any[0, "gdp"], Any[1, "match id"], Any[1, "date"], Any[1, "venue"], Any[1, "score"], Any[1, "result"], Any[1, "competition"], Any[2, "city id"], Any[2, "jan"], Any[2, "feb"], Any[2, "mar"], Any[2, "apr"], Any[2, "jun"], Any[2, "jul"], Any[2, "aug"], Any[2, "sep"], Any[2, "oct"], Any[2, "nov"], Any[2, "dec"], Any[3, "year"], Any[3, "match id"], Any[3, "host city"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 1], Any[26, 7], Any[27, 1]])
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







PClean.@model CityRecordModel begin
    @class City begin
        city ~ ChooseUniformly(possibilities[:city])
        hanzi ~ ChooseUniformly(possibilities[:hanzi])
        hanyu_pinyin ~ ChooseUniformly(possibilities[:hanyu_pinyin])
        regional_population ~ ChooseUniformly(possibilities[:regional_population])
        gdp ~ ChooseUniformly(possibilities[:gdp])
    end

    @class Match begin
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        score ~ ChooseUniformly(possibilities[:score])
        result ~ ChooseUniformly(possibilities[:result])
        competition ~ ChooseUniformly(possibilities[:competition])
    end

    @class Temperature begin
        jan ~ ChooseUniformly(possibilities[:jan])
        feb ~ ChooseUniformly(possibilities[:feb])
        mar ~ ChooseUniformly(possibilities[:mar])
        apr ~ ChooseUniformly(possibilities[:apr])
        jun ~ ChooseUniformly(possibilities[:jun])
        jul ~ ChooseUniformly(possibilities[:jul])
        aug ~ ChooseUniformly(possibilities[:aug])
        sep ~ ChooseUniformly(possibilities[:sep])
        oct ~ ChooseUniformly(possibilities[:oct])
        nov ~ ChooseUniformly(possibilities[:nov])
        dec ~ ChooseUniformly(possibilities[:dec])
    end

    @class Hosting_city begin
        year ~ ChooseUniformly(possibilities[:year])
        match ~ Match
        city ~ City
    end

    @class Obs begin
        temperature ~ Temperature
        hosting_city ~ Hosting_city
    end
end

query = @query CityRecordModel.Obs [
    city_id temperature.city.city_id
    city temperature.city.city
    city_hanzi temperature.city.hanzi
    city_hanyu_pinyin temperature.city.hanyu_pinyin
    city_regional_population temperature.city.regional_population
    city_gdp temperature.city.gdp
    match_id hosting_city.match.match_id
    match_date hosting_city.match.date
    match_venue hosting_city.match.venue
    match_score hosting_city.match.score
    match_result hosting_city.match.result
    match_competition hosting_city.match.competition
    temperature_jan temperature.jan
    temperature_feb temperature.feb
    temperature_mar temperature.mar
    temperature_apr temperature.apr
    temperature_jun temperature.jun
    temperature_jul temperature.jul
    temperature_aug temperature.aug
    temperature_sep temperature.sep
    temperature_oct temperature.oct
    temperature_nov temperature.nov
    temperature_dec temperature.dec
    hosting_city_year hosting_city.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
