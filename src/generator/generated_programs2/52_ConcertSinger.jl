using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("stadium_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("stadium_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "stadium id"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "singer id"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert id"], Any[2, "concert name"], Any[2, "theme"], Any[2, "stadium id"], Any[2, "year"], Any[3, "concert id"], Any[3, "singer id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "stadium id"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "singer id"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert id"], Any[2, "concert name"], Any[2, "theme"], Any[2, "stadium id"], Any[2, "year"], Any[3, "concert id"], Any[3, "singer id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "stadium id"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "singer id"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert id"], Any[2, "concert name"], Any[2, "theme"], Any[2, "stadium id"], Any[2, "year"], Any[3, "concert id"], Any[3, "singer id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "stadium id"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "singer id"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert id"], Any[2, "concert name"], Any[2, "theme"], Any[2, "stadium id"], Any[2, "year"], Any[3, "concert id"], Any[3, "singer id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "stadium id"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "singer id"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert id"], Any[2, "concert name"], Any[2, "theme"], Any[2, "stadium id"], Any[2, "year"], Any[3, "concert id"], Any[3, "singer id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[18, 1], Any[21, 8], Any[20, 15]])
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







PClean.@model ConcertSingerModel begin
    @class Stadium begin
        location ~ ChooseUniformly(possibilities[:location])
        name ~ ChooseUniformly(possibilities[:name])
        capacity ~ ChooseUniformly(possibilities[:capacity])
        highest ~ ChooseUniformly(possibilities[:highest])
        lowest ~ ChooseUniformly(possibilities[:lowest])
        average ~ ChooseUniformly(possibilities[:average])
    end

    @class Singer begin
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        song_name ~ ChooseUniformly(possibilities[:song_name])
        song_release_year ~ ChooseUniformly(possibilities[:song_release_year])
        age ~ ChooseUniformly(possibilities[:age])
        is_male ~ ChooseUniformly(possibilities[:is_male])
    end

    @class Concert begin
        concert_name ~ ChooseUniformly(possibilities[:concert_name])
        theme ~ ChooseUniformly(possibilities[:theme])
        stadium ~ Stadium
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Singer_in_concert begin
        singer ~ Singer
    end

    @class Obs begin
        singer_in_concert ~ Singer_in_concert
    end
end

query = @query ConcertSingerModel.Obs [
    stadium_id singer_in_concert.concert.stadium.stadium_id
    stadium_location singer_in_concert.concert.stadium.location
    stadium_name singer_in_concert.concert.stadium.name
    stadium_capacity singer_in_concert.concert.stadium.capacity
    stadium_highest singer_in_concert.concert.stadium.highest
    stadium_lowest singer_in_concert.concert.stadium.lowest
    stadium_average singer_in_concert.concert.stadium.average
    singer_id singer_in_concert.singer.singer_id
    singer_name singer_in_concert.singer.name
    singer_country singer_in_concert.singer.country
    singer_song_name singer_in_concert.singer.song_name
    singer_song_release_year singer_in_concert.singer.song_release_year
    singer_age singer_in_concert.singer.age
    singer_is_male singer_in_concert.singer.is_male
    concert_id singer_in_concert.concert.concert_id
    concert_name singer_in_concert.concert.concert_name
    concert_theme singer_in_concert.concert.theme
    concert_year singer_in_concert.concert.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
