using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("genre_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("genre_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "genre name"], Any[0, "rating"], Any[0, "most popular in"], Any[1, "artist name"], Any[1, "country"], Any[1, "gender"], Any[1, "preferred genre"], Any[2, "song id"], Any[2, "artist name"], Any[2, "file size"], Any[2, "duration"], Any[2, "formats"], Any[3, "song name"], Any[3, "artist name"], Any[3, "country"], Any[3, "song id"], Any[3, "genre is"], Any[3, "rating"], Any[3, "languages"], Any[3, "releasedate"], Any[3, "resolution"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[9, 4], Any[17, 1], Any[16, 8], Any[14, 4]])
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
        genre ~ Genre
    end

    @class Files begin
        artist ~ Artist
        file_size ~ ChooseUniformly(possibilities[:file_size])
        duration ~ ChooseUniformly(possibilities[:duration])
        formats ~ ChooseUniformly(possibilities[:formats])
    end

    @class Song begin
        song_name ~ ChooseUniformly(possibilities[:song_name])
        artist ~ Artist
        country ~ ChooseUniformly(possibilities[:country])
        files ~ Files
        genre ~ Genre
        rating ~ ChooseUniformly(possibilities[:rating])
        languages ~ ChooseUniformly(possibilities[:languages])
        releasedate ~ TimePrior(possibilities[:releasedate])
        resolution ~ ChooseUniformly(possibilities[:resolution])
    end

    @class Obs begin
        song ~ Song
    end
end

query = @query Music1Model.Obs [
    genre_name song.artist.genre.genre_name
    genre_rating song.artist.genre.rating
    genre_most_popular_in song.artist.genre.most_popular_in
    artist_name song.files.artist.artist_name
    artist_country song.files.artist.country
    artist_gender song.files.artist.gender
    files_song_id song.files.song_id
    files_file_size song.files.file_size
    files_duration song.files.duration
    files_formats song.files.formats
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
