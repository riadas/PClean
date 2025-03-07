using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("film_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("film_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "film id"], Any[0, "title"], Any[0, "studio"], Any[0, "director"], Any[0, "gross in dollar"], Any[1, "market id"], Any[1, "country"], Any[1, "number cities"], Any[2, "estimation id"], Any[2, "low estimate"], Any[2, "high estimate"], Any[2, "film id"], Any[2, "type"], Any[2, "market id"], Any[2, "year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "film id"], Any[0, "title"], Any[0, "studio"], Any[0, "director"], Any[0, "gross in dollar"], Any[1, "market id"], Any[1, "country"], Any[1, "number cities"], Any[2, "estimation id"], Any[2, "low estimate"], Any[2, "high estimate"], Any[2, "film id"], Any[2, "type"], Any[2, "market id"], Any[2, "year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "film id"], Any[0, "title"], Any[0, "studio"], Any[0, "director"], Any[0, "gross in dollar"], Any[1, "market id"], Any[1, "country"], Any[1, "number cities"], Any[2, "estimation id"], Any[2, "low estimate"], Any[2, "high estimate"], Any[2, "film id"], Any[2, "type"], Any[2, "market id"], Any[2, "year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "film id"], Any[0, "title"], Any[0, "studio"], Any[0, "director"], Any[0, "gross in dollar"], Any[1, "market id"], Any[1, "country"], Any[1, "number cities"], Any[2, "estimation id"], Any[2, "low estimate"], Any[2, "high estimate"], Any[2, "film id"], Any[2, "type"], Any[2, "market id"], Any[2, "year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "film id"], Any[0, "title"], Any[0, "studio"], Any[0, "director"], Any[0, "gross in dollar"], Any[1, "market id"], Any[1, "country"], Any[1, "number cities"], Any[2, "estimation id"], Any[2, "low estimate"], Any[2, "high estimate"], Any[2, "film id"], Any[2, "type"], Any[2, "market id"], Any[2, "year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[14, 6], Any[12, 1]])
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







PClean.@model FilmRankModel begin
    @class Film begin
        title ~ ChooseUniformly(possibilities[:title])
        studio ~ ChooseUniformly(possibilities[:studio])
        director ~ ChooseUniformly(possibilities[:director])
        gross_in_dollar ~ ChooseUniformly(possibilities[:gross_in_dollar])
    end

    @class Market begin
        country ~ ChooseUniformly(possibilities[:country])
        number_cities ~ ChooseUniformly(possibilities[:number_cities])
    end

    @class Film_market_estimation begin
        low_estimate ~ ChooseUniformly(possibilities[:low_estimate])
        high_estimate ~ ChooseUniformly(possibilities[:high_estimate])
        film ~ Film
        type ~ ChooseUniformly(possibilities[:type])
        market ~ Market
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        film_market_estimation ~ Film_market_estimation
    end
end

query = @query FilmRankModel.Obs [
    film_id film_market_estimation.film.film_id
    film_title film_market_estimation.film.title
    film_studio film_market_estimation.film.studio
    film_director film_market_estimation.film.director
    film_gross_in_dollar film_market_estimation.film.gross_in_dollar
    market_id film_market_estimation.market.market_id
    market_country film_market_estimation.market.country
    market_number_cities film_market_estimation.market.number_cities
    film_market_estimation_estimation_id film_market_estimation.estimation_id
    film_market_estimation_low_estimate film_market_estimation.low_estimate
    film_market_estimation_high_estimate film_market_estimation.high_estimate
    film_market_estimation_type film_market_estimation.type
    film_market_estimation_year film_market_estimation.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
