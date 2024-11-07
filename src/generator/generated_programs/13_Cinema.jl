using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("film_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("film_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "film id"], Any[0, "rank in series"], Any[0, "number in season"], Any[0, "title"], Any[0, "directed by"], Any[0, "original air date"], Any[0, "production code"], Any[1, "cinema id"], Any[1, "name"], Any[1, "openning year"], Any[1, "capacity"], Any[1, "location"], Any[2, "cinema id"], Any[2, "film id"], Any[2, "date"], Any[2, "show times per day"], Any[2, "price"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "film id"], Any[0, "rank in series"], Any[0, "number in season"], Any[0, "title"], Any[0, "directed by"], Any[0, "original air date"], Any[0, "production code"], Any[1, "cinema id"], Any[1, "name"], Any[1, "openning year"], Any[1, "capacity"], Any[1, "location"], Any[2, "cinema id"], Any[2, "film id"], Any[2, "date"], Any[2, "show times per day"], Any[2, "price"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model CinemaModel begin
    @class Film begin
        film_id ~ Unmodeled()
        rank_in_series ~ ChooseUniformly(possibilities[:rank_in_series])
        number_in_season ~ ChooseUniformly(possibilities[:number_in_season])
        title ~ ChooseUniformly(possibilities[:title])
        directed_by ~ ChooseUniformly(possibilities[:directed_by])
        original_air_date ~ ChooseUniformly(possibilities[:original_air_date])
        production_code ~ ChooseUniformly(possibilities[:production_code])
    end

    @class Cinema begin
        cinema_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        openning_year ~ ChooseUniformly(possibilities[:openning_year])
        capacity ~ ChooseUniformly(possibilities[:capacity])
        location ~ ChooseUniformly(possibilities[:location])
    end

    @class Schedule begin
        cinema_id ~ Unmodeled()
        film_id ~ ChooseUniformly(possibilities[:film_id])
        date ~ ChooseUniformly(possibilities[:date])
        show_times_per_day ~ ChooseUniformly(possibilities[:show_times_per_day])
        price ~ ChooseUniformly(possibilities[:price])
    end

    @class Obs begin
        film ~ Film
        cinema ~ Cinema
        schedule ~ Schedule
    end
end

query = @query CinemaModel.Obs [
    film_id film.film_id
    film_rank_in_series film.rank_in_series
    film_number_in_season film.number_in_season
    film_title film.title
    film_directed_by film.directed_by
    film_original_air_date film.original_air_date
    film_production_code film.production_code
    cinema_id cinema.cinema_id
    cinema_name cinema.name
    cinema_openning_year cinema.openning_year
    cinema_capacity cinema.capacity
    cinema_location cinema.location
    schedule_date schedule.date
    schedule_show_times_per_day schedule.show_times_per_day
    schedule_price schedule.price
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
