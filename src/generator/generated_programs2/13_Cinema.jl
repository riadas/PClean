using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("film_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("film_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "film id"], Any[0, "rank in series"], Any[0, "number in season"], Any[0, "title"], Any[0, "directed by"], Any[0, "original air date"], Any[0, "production code"], Any[1, "cinema id"], Any[1, "name"], Any[1, "openning year"], Any[1, "capacity"], Any[1, "location"], Any[2, "cinema id"], Any[2, "film id"], Any[2, "date"], Any[2, "show times per day"], Any[2, "price"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "film id"], Any[0, "rank in series"], Any[0, "number in season"], Any[0, "title"], Any[0, "directed by"], Any[0, "original air date"], Any[0, "production code"], Any[1, "cinema id"], Any[1, "name"], Any[1, "openning year"], Any[1, "capacity"], Any[1, "location"], Any[2, "cinema id"], Any[2, "film id"], Any[2, "date"], Any[2, "show times per day"], Any[2, "price"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["cinema id", "film id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "rank in series"], Any[0, "number in season"], Any[0, "title"], Any[0, "directed by"], Any[0, "original air date"], Any[0, "production code"], Any[1, "name"], Any[1, "openning year"], Any[1, "capacity"], Any[1, "location"], Any[2, "date"], Any[2, "show times per day"], Any[2, "price"]]
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
        cinema ~ Cinema
        film ~ Film
        date ~ ChooseUniformly(possibilities[:date])
        show_times_per_day ~ ChooseUniformly(possibilities[:show_times_per_day])
        price ~ ChooseUniformly(possibilities[:price])
    end

    @class Obs begin
        schedule ~ Schedule
    end
end

query = @query CinemaModel.Obs [
    film_id schedule.film.film_id
    film_rank_in_series schedule.film.rank_in_series
    film_number_in_season schedule.film.number_in_season
    film_title schedule.film.title
    film_directed_by schedule.film.directed_by
    film_original_air_date schedule.film.original_air_date
    film_production_code schedule.film.production_code
    cinema_id schedule.cinema.cinema_id
    cinema_name schedule.cinema.name
    cinema_openning_year schedule.cinema.openning_year
    cinema_capacity schedule.cinema.capacity
    cinema_location schedule.cinema.location
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
