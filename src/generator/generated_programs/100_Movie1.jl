using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("movie_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("movie_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "movie id"], Any[0, "title"], Any[0, "year"], Any[0, "director"], Any[1, "reviewer id"], Any[1, "name"], Any[2, "reviewer id"], Any[2, "movie id"], Any[2, "rating stars"], Any[2, "rating date"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "movie id"], Any[0, "title"], Any[0, "year"], Any[0, "director"], Any[1, "reviewer id"], Any[1, "name"], Any[2, "reviewer id"], Any[2, "movie id"], Any[2, "rating stars"], Any[2, "rating date"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Movie1Model begin
    @class Movie begin
        movie_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
        director ~ ChooseUniformly(possibilities[:director])
    end

    @class Reviewer begin
        reviewer_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Rating begin
        reviewer_id ~ Unmodeled()
        movie_id ~ ChooseUniformly(possibilities[:movie_id])
        rating_stars ~ ChooseUniformly(possibilities[:rating_stars])
        rating_date ~ TimePrior(possibilities[:rating_date])
    end

    @class Obs begin
        movie ~ Movie
        reviewer ~ Reviewer
        rating ~ Rating
    end
end

query = @query Movie1Model.Obs [
    movie_id movie.movie_id
    movie_title movie.title
    movie_year movie.year
    movie_director movie.director
    reviewer_id reviewer.reviewer_id
    reviewer_name reviewer.name
    rating_stars rating.rating_stars
    rating_date rating.rating_date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
