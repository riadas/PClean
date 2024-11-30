using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("artist_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("artist_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "artist"], Any[0, "age"], Any[0, "famous title"], Any[0, "famous release date"], Any[1, "volume id"], Any[1, "volume issue"], Any[1, "issue date"], Any[1, "weeks on top"], Any[1, "song"], Any[1, "artist id"], Any[2, "id"], Any[2, "music festival"], Any[2, "date of ceremony"], Any[2, "category"], Any[2, "volume"], Any[2, "result"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "artist"], Any[0, "age"], Any[0, "famous title"], Any[0, "famous release date"], Any[1, "volume id"], Any[1, "volume issue"], Any[1, "issue date"], Any[1, "weeks on top"], Any[1, "song"], Any[1, "artist id"], Any[2, "id"], Any[2, "music festival"], Any[2, "date of ceremony"], Any[2, "category"], Any[2, "volume"], Any[2, "result"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["artist id", "volume"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "artist"], Any[0, "age"], Any[0, "famous title"], Any[0, "famous release date"], Any[1, "volume id"], Any[1, "volume issue"], Any[1, "issue date"], Any[1, "weeks on top"], Any[1, "song"], Any[2, "id"], Any[2, "music festival"], Any[2, "date of ceremony"], Any[2, "category"], Any[2, "result"]]
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





PClean.@model Music4Model begin
    @class Artist begin
        artist_id ~ Unmodeled()
        artist ~ ChooseUniformly(possibilities[:artist])
        age ~ ChooseUniformly(possibilities[:age])
        famous_title ~ ChooseUniformly(possibilities[:famous_title])
        famous_release_date ~ ChooseUniformly(possibilities[:famous_release_date])
    end

    @class Volume begin
        volume_id ~ Unmodeled()
        volume_issue ~ ChooseUniformly(possibilities[:volume_issue])
        issue_date ~ ChooseUniformly(possibilities[:issue_date])
        weeks_on_top ~ ChooseUniformly(possibilities[:weeks_on_top])
        song ~ ChooseUniformly(possibilities[:song])
        artist ~ Artist
    end

    @class Music_Festival begin
        id ~ Unmodeled()
        music_festival ~ ChooseUniformly(possibilities[:music_festival])
        date_of_ceremony ~ ChooseUniformly(possibilities[:date_of_ceremony])
        category ~ ChooseUniformly(possibilities[:category])
        volume ~ Volume
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Obs begin
        music_Festival ~ Music_Festival
    end
end

query = @query Music4Model.Obs [
    artist_id music_Festival.volume.artist.artist_id
    artist music_Festival.volume.artist.artist
    artist_age music_Festival.volume.artist.age
    artist_famous_title music_Festival.volume.artist.famous_title
    artist_famous_release_date music_Festival.volume.artist.famous_release_date
    volume_id music_Festival.volume.volume_id
    volume_issue music_Festival.volume.volume_issue
    volume_issue_date music_Festival.volume.issue_date
    volume_weeks_on_top music_Festival.volume.weeks_on_top
    volume_song music_Festival.volume.song
    music_festival_id music_Festival.id
    music_festival music_Festival.music_festival
    music_festival_date_of_ceremony music_Festival.date_of_ceremony
    music_festival_category music_Festival.category
    music_festival_result music_Festival.result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))