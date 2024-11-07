using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("film_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("film_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "film id"], Any[0, "title"], Any[0, "studio"], Any[0, "director"], Any[0, "gross in dollar"], Any[1, "market id"], Any[1, "country"], Any[1, "number cities"], Any[2, "estimation id"], Any[2, "low estimate"], Any[2, "high estimate"], Any[2, "film id"], Any[2, "type"], Any[2, "market id"], Any[2, "year"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "film id"], Any[0, "title"], Any[0, "studio"], Any[0, "director"], Any[0, "gross in dollar"], Any[1, "market id"], Any[1, "country"], Any[1, "number cities"], Any[2, "estimation id"], Any[2, "low estimate"], Any[2, "high estimate"], Any[2, "film id"], Any[2, "type"], Any[2, "market id"], Any[2, "year"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model FilmRankModel begin
    @class Film begin
        film_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        studio ~ ChooseUniformly(possibilities[:studio])
        director ~ ChooseUniformly(possibilities[:director])
        gross_in_dollar ~ ChooseUniformly(possibilities[:gross_in_dollar])
    end

    @class Market begin
        market_id ~ Unmodeled()
        country ~ ChooseUniformly(possibilities[:country])
        number_cities ~ ChooseUniformly(possibilities[:number_cities])
    end

    @class Film_Market_Estimation begin
        estimation_id ~ Unmodeled()
        low_estimate ~ ChooseUniformly(possibilities[:low_estimate])
        high_estimate ~ ChooseUniformly(possibilities[:high_estimate])
        film_id ~ ChooseUniformly(possibilities[:film_id])
        type ~ ChooseUniformly(possibilities[:type])
        market_id ~ ChooseUniformly(possibilities[:market_id])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        film ~ Film
        market ~ Market
        film_Market_Estimation ~ Film_Market_Estimation
    end
end

query = @query FilmRankModel.Obs [
    film_id film.film_id
    film_title film.title
    film_studio film.studio
    film_director film.director
    film_gross_in_dollar film.gross_in_dollar
    market_id market.market_id
    market_country market.country
    market_number_cities market.number_cities
    film_market_estimation_estimation_id film_Market_Estimation.estimation_id
    film_market_estimation_low_estimate film_Market_Estimation.low_estimate
    film_market_estimation_high_estimate film_Market_Estimation.high_estimate
    film_market_estimation_type film_Market_Estimation.type
    film_market_estimation_year film_Market_Estimation.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
