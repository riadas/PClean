using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("genre_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("genre_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "genre name"], Any[0, "rating"], Any[0, "most popular in"], Any[1, "artist name"], Any[1, "country"], Any[1, "gender"], Any[1, "preferred genre"], Any[2, "song id"], Any[2, "artist name"], Any[2, "file size"], Any[2, "duration"], Any[2, "formats"], Any[3, "song name"], Any[3, "artist name"], Any[3, "country"], Any[3, "song id"], Any[3, "genre is"], Any[3, "rating"], Any[3, "languages"], Any[3, "releasedate"], Any[3, "resolution"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "genre name"], Any[0, "rating"], Any[0, "most popular in"], Any[1, "artist name"], Any[1, "country"], Any[1, "gender"], Any[1, "preferred genre"], Any[2, "song id"], Any[2, "artist name"], Any[2, "file size"], Any[2, "duration"], Any[2, "formats"], Any[3, "song name"], Any[3, "artist name"], Any[3, "country"], Any[3, "song id"], Any[3, "genre is"], Any[3, "rating"], Any[3, "languages"], Any[3, "releasedate"], Any[3, "resolution"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["preferred genre", "artist name", "genre is", "song id", "artist name"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "genre name"], Any[0, "rating"], Any[0, "most popular in"], Any[1, "country"], Any[1, "gender"], Any[2, "file size"], Any[2, "duration"], Any[2, "formats"], Any[3, "song name"], Any[3, "country"], Any[3, "rating"], Any[3, "languages"], Any[3, "releasedate"], Any[3, "resolution"]]
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





PClean.@model Music1Model begin
    @class Genre begin
        genre_name ~ ChooseUniformly(possibilities[:genre_name])
        rating ~ ChooseUniformly(possibilities[:rating])
        most_popular_in ~ ChooseUniformly(possibilities[:most_popular_in])
    end

    @class Obs begin
        genre ~ Genre
        artist_name ~ ChooseUniformly(possibilities[:artist_name])
        country ~ ChooseUniformly(possibilities[:country])
        gender ~ ChooseUniformly(possibilities[:gender])
        song_id ~ Unmodeled()
        file_size ~ ChooseUniformly(possibilities[:file_size])
        duration ~ ChooseUniformly(possibilities[:duration])
        formats ~ ChooseUniformly(possibilities[:formats])
        song_name ~ ChooseUniformly(possibilities[:song_name])
        country ~ ChooseUniformly(possibilities[:country])
        rating ~ ChooseUniformly(possibilities[:rating])
        languages ~ ChooseUniformly(possibilities[:languages])
        releasedate ~ TimePrior(possibilities[:releasedate])
        resolution ~ ChooseUniformly(possibilities[:resolution])
    end
end

query = @query Music1Model.Obs [
    genre_name genre.genre_name
    genre_rating genre.rating
    genre_most_popular_in genre.most_popular_in
    artist_name artist_name
    artist_country country
    artist_gender gender
    files_song_id song_id
    files_file_size file_size
    files_duration duration
    files_formats formats
    song_name song_name
    song_country country
    song_rating rating
    song_languages languages
    song_releasedate releasedate
    song_resolution resolution
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
