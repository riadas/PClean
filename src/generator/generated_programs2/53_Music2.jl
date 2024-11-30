using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("songs_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("songs_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "song id"], Any[0, "title"], Any[1, "aid"], Any[1, "title"], Any[1, "year"], Any[1, "label"], Any[1, "type"], Any[2, "id"], Any[2, "first name"], Any[2, "last name"], Any[3, "song id"], Any[3, "bandmate id"], Any[3, "instrument"], Any[4, "song id"], Any[4, "bandmate"], Any[4, "stage position"], Any[5, "album id"], Any[5, "position"], Any[5, "song id"], Any[6, "song id"], Any[6, "bandmate"], Any[6, "type"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "song id"], Any[0, "title"], Any[1, "aid"], Any[1, "title"], Any[1, "year"], Any[1, "label"], Any[1, "type"], Any[2, "id"], Any[2, "first name"], Any[2, "last name"], Any[3, "song id"], Any[3, "bandmate id"], Any[3, "instrument"], Any[4, "song id"], Any[4, "bandmate"], Any[4, "stage position"], Any[5, "album id"], Any[5, "position"], Any[5, "song id"], Any[6, "song id"], Any[6, "bandmate"], Any[6, "type"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["bandmate id", "song id", "bandmate", "song id", "album id", "song id", "bandmate", "song id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "title"], Any[1, "aid"], Any[1, "title"], Any[1, "year"], Any[1, "label"], Any[1, "type"], Any[2, "id"], Any[2, "first name"], Any[2, "last name"], Any[3, "instrument"], Any[4, "stage position"], Any[5, "position"], Any[6, "type"]]
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





PClean.@model Music2Model begin
    @class Songs begin
        song_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
    end

    @class Albums begin
        aid ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
        label ~ ChooseUniformly(possibilities[:label])
        type ~ ChooseUniformly(possibilities[:type])
    end

    @class Band begin
        id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
    end

    @class Instruments begin
        songs ~ Songs
        band ~ Band
        instrument ~ ChooseUniformly(possibilities[:instrument])
    end

    @class Performance begin
        songs ~ Songs
        band ~ Band
        stage_position ~ ChooseUniformly(possibilities[:stage_position])
    end

    @class Track_Lists begin
        albums ~ Albums
        position ~ ChooseUniformly(possibilities[:position])
        songs ~ Songs
    end

    @class Vocals begin
        songs ~ Songs
        band ~ Band
        type ~ ChooseUniformly(possibilities[:type])
    end

    @class Obs begin
        instruments ~ Instruments
        performance ~ Performance
        track_Lists ~ Track_Lists
        vocals ~ Vocals
    end
end

query = @query Music2Model.Obs [
    songs_song_id instruments.songs.song_id
    songs_title instruments.songs.title
    albums_aid track_Lists.albums.aid
    albums_title track_Lists.albums.title
    albums_year track_Lists.albums.year
    albums_label track_Lists.albums.label
    albums_type track_Lists.albums.type
    band_id instruments.band.id
    band_first_name instruments.band.first_name
    band_last_name instruments.band.last_name
    instruments_instrument instruments.instrument
    performance_stage_position performance.stage_position
    track_lists_position track_Lists.position
    vocals_type vocals.type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
