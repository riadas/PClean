using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("stadium_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("stadium_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "stadium id"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "singer id"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert id"], Any[2, "concert name"], Any[2, "theme"], Any[2, "stadium id"], Any[2, "year"], Any[3, "concert id"], Any[3, "singer id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "stadium id"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "singer id"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert id"], Any[2, "concert name"], Any[2, "theme"], Any[2, "stadium id"], Any[2, "year"], Any[3, "concert id"], Any[3, "singer id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ConcertSingerModel begin
    @class Stadium begin
        stadium_id ~ Unmodeled()
        location ~ ChooseUniformly(possibilities[:location])
        name ~ ChooseUniformly(possibilities[:name])
        capacity ~ ChooseUniformly(possibilities[:capacity])
        highest ~ ChooseUniformly(possibilities[:highest])
        lowest ~ ChooseUniformly(possibilities[:lowest])
        average ~ ChooseUniformly(possibilities[:average])
    end

    @class Singer begin
        singer_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        song_name ~ ChooseUniformly(possibilities[:song_name])
        song_release_year ~ ChooseUniformly(possibilities[:song_release_year])
        age ~ ChooseUniformly(possibilities[:age])
        is_male ~ ChooseUniformly(possibilities[:is_male])
    end

    @class Concert begin
        concert_id ~ Unmodeled()
        concert_name ~ ChooseUniformly(possibilities[:concert_name])
        theme ~ ChooseUniformly(possibilities[:theme])
        stadium_id ~ ChooseUniformly(possibilities[:stadium_id])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Singer_In_Concert begin
        concert_id ~ Unmodeled()
        singer_id ~ ChooseUniformly(possibilities[:singer_id])
    end

    @class Obs begin
        stadium ~ Stadium
        singer ~ Singer
        concert ~ Concert
        singer_In_Concert ~ Singer_In_Concert
    end
end

query = @query ConcertSingerModel.Obs [
    stadium_id stadium.stadium_id
    stadium_location stadium.location
    stadium_name stadium.name
    stadium_capacity stadium.capacity
    stadium_highest stadium.highest
    stadium_lowest stadium.lowest
    stadium_average stadium.average
    singer_id singer.singer_id
    singer_name singer.name
    singer_country singer.country
    singer_song_name singer.song_name
    singer_song_release_year singer.song_release_year
    singer_age singer.age
    singer_is_male singer.is_male
    concert_id concert.concert_id
    concert_name concert.concert_name
    concert_theme concert.theme
    concert_year concert.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
