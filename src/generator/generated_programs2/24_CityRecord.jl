using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("city_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("city_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


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
foreign_keys = ["city id", "match id", "host city"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "city"], Any[0, "hanzi"], Any[0, "hanyu pinyin"], Any[0, "regional population"], Any[0, "gdp"], Any[1, "date"], Any[1, "venue"], Any[1, "score"], Any[1, "result"], Any[1, "competition"], Any[2, "jan"], Any[2, "feb"], Any[2, "mar"], Any[2, "apr"], Any[2, "jun"], Any[2, "jul"], Any[2, "aug"], Any[2, "sep"], Any[2, "oct"], Any[2, "nov"], Any[2, "dec"], Any[3, "year"]]
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





PClean.@model CityRecordModel begin
    @class City begin
        city_id ~ Unmodeled()
        city ~ ChooseUniformly(possibilities[:city])
        hanzi ~ ChooseUniformly(possibilities[:hanzi])
        hanyu_pinyin ~ ChooseUniformly(possibilities[:hanyu_pinyin])
        regional_population ~ ChooseUniformly(possibilities[:regional_population])
        gdp ~ ChooseUniformly(possibilities[:gdp])
    end

    @class Match begin
        match_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        score ~ ChooseUniformly(possibilities[:score])
        result ~ ChooseUniformly(possibilities[:result])
        competition ~ ChooseUniformly(possibilities[:competition])
    end

    @class Temperature begin
        city ~ City
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

    @class Hosting_City begin
        year ~ ChooseUniformly(possibilities[:year])
        match ~ Match
        city ~ City
    end

    @class Obs begin
        temperature ~ Temperature
        hosting_City ~ Hosting_City
    end
end

query = @query CityRecordModel.Obs [
    city_id temperature.city.city_id
    city temperature.city.city
    city_hanzi temperature.city.hanzi
    city_hanyu_pinyin temperature.city.hanyu_pinyin
    city_regional_population temperature.city.regional_population
    city_gdp temperature.city.gdp
    match_id hosting_City.match.match_id
    match_date hosting_City.match.date
    match_venue hosting_City.match.venue
    match_score hosting_City.match.score
    match_result hosting_City.match.result
    match_competition hosting_City.match.competition
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
    hosting_city_year hosting_City.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
