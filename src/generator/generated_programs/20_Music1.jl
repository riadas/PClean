using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("genre_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("genre_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "genre name"], Any[0, "rating"], Any[0, "most popular in"], Any[1, "artist name"], Any[1, "country"], Any[1, "gender"], Any[1, "preferred genre"], Any[2, "song id"], Any[2, "artist name"], Any[2, "file size"], Any[2, "duration"], Any[2, "formats"], Any[3, "song name"], Any[3, "artist name"], Any[3, "country"], Any[3, "song id"], Any[3, "genre is"], Any[3, "rating"], Any[3, "languages"], Any[3, "releasedate"], Any[3, "resolution"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "genre name"], Any[0, "rating"], Any[0, "most popular in"], Any[1, "artist name"], Any[1, "country"], Any[1, "gender"], Any[1, "preferred genre"], Any[2, "song id"], Any[2, "artist name"], Any[2, "file size"], Any[2, "duration"], Any[2, "formats"], Any[3, "song name"], Any[3, "artist name"], Any[3, "country"], Any[3, "song id"], Any[3, "genre is"], Any[3, "rating"], Any[3, "languages"], Any[3, "releasedate"], Any[3, "resolution"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Artist begin
        artist_name ~ ChooseUniformly(possibilities[:artist_name])
        country ~ ChooseUniformly(possibilities[:country])
        gender ~ ChooseUniformly(possibilities[:gender])
        preferred_genre ~ ChooseUniformly(possibilities[:preferred_genre])
    end

    @class Files begin
        song_id ~ Unmodeled()
        artist_name ~ ChooseUniformly(possibilities[:artist_name])
        file_size ~ ChooseUniformly(possibilities[:file_size])
        duration ~ ChooseUniformly(possibilities[:duration])
        formats ~ ChooseUniformly(possibilities[:formats])
    end

    @class Song begin
        song_name ~ ChooseUniformly(possibilities[:song_name])
        artist_name ~ ChooseUniformly(possibilities[:artist_name])
        country ~ ChooseUniformly(possibilities[:country])
        song_id ~ ChooseUniformly(possibilities[:song_id])
        genre_is ~ ChooseUniformly(possibilities[:genre_is])
        rating ~ ChooseUniformly(possibilities[:rating])
        languages ~ ChooseUniformly(possibilities[:languages])
        releasedate ~ TimePrior(possibilities[:releasedate])
        resolution ~ ChooseUniformly(possibilities[:resolution])
    end

    @class Obs begin
        genre ~ Genre
        artist ~ Artist
        files ~ Files
        song ~ Song
    end
end

query = @query Music1Model.Obs [
    genre_name genre.genre_name
    genre_rating genre.rating
    genre_most_popular_in genre.most_popular_in
    artist_name artist.artist_name
    artist_country artist.country
    artist_gender artist.gender
    files_song_id files.song_id
    files_file_size files.file_size
    files_duration files.duration
    files_formats files.formats
    song_name song.song_name
    song_country song.country
    song_rating song.rating
    song_languages song.languages
    song_releasedate song.releasedate
    song_resolution song.resolution
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
