using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("artist_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("artist_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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
cols = Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "artist"], Any[0, "age"], Any[0, "famous title"], Any[0, "famous release date"], Any[1, "volume id"], Any[1, "volume issue"], Any[1, "issue date"], Any[1, "weeks on top"], Any[1, "song"], Any[1, "artist id"], Any[2, "id"], Any[2, "music festival"], Any[2, "date of ceremony"], Any[2, "category"], Any[2, "volume"], Any[2, "result"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 1], Any[16, 6]])
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







PClean.@model Music4Model begin
    @class Artist begin
        artist ~ ChooseUniformly(possibilities[:artist])
        age ~ ChooseUniformly(possibilities[:age])
        famous_title ~ ChooseUniformly(possibilities[:famous_title])
        famous_release_date ~ ChooseUniformly(possibilities[:famous_release_date])
    end

    @class Volume begin
        volume_issue ~ ChooseUniformly(possibilities[:volume_issue])
        issue_date ~ ChooseUniformly(possibilities[:issue_date])
        weeks_on_top ~ ChooseUniformly(possibilities[:weeks_on_top])
        song ~ ChooseUniformly(possibilities[:song])
        artist ~ Artist
    end

    @class Music_festival begin
        music_festival ~ ChooseUniformly(possibilities[:music_festival])
        date_of_ceremony ~ ChooseUniformly(possibilities[:date_of_ceremony])
        category ~ ChooseUniformly(possibilities[:category])
        volume ~ Volume
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Obs begin
        music_festival ~ Music_festival
    end
end

query = @query Music4Model.Obs [
    artist_id music_festival.volume.artist.artist_id
    artist music_festival.volume.artist.artist
    artist_age music_festival.volume.artist.age
    artist_famous_title music_festival.volume.artist.famous_title
    artist_famous_release_date music_festival.volume.artist.famous_release_date
    volume_id music_festival.volume.volume_id
    volume_issue music_festival.volume.volume_issue
    volume_issue_date music_festival.volume.issue_date
    volume_weeks_on_top music_festival.volume.weeks_on_top
    volume_song music_festival.volume.song
    music_festival music_festival.music_festival
    music_festival_date_of_ceremony music_festival.date_of_ceremony
    music_festival_category music_festival.category
    music_festival_result music_festival.result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
