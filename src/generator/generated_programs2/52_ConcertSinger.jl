using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("stadium_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("stadium_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


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
foreign_keys = ["stadium id", "singer id", "concert id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "location"], Any[0, "name"], Any[0, "capacity"], Any[0, "highest"], Any[0, "lowest"], Any[0, "average"], Any[1, "name"], Any[1, "country"], Any[1, "song name"], Any[1, "song release year"], Any[1, "age"], Any[1, "is male"], Any[2, "concert name"], Any[2, "theme"], Any[2, "year"]]
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
        stadium ~ Stadium
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Singer_In_Concert begin
        concert ~ Concert
        singer ~ Singer
    end

    @class Obs begin
        singer_In_Concert ~ Singer_In_Concert
    end
end

query = @query ConcertSingerModel.Obs [
    stadium_id singer_In_Concert.concert.stadium.stadium_id
    stadium_location singer_In_Concert.concert.stadium.location
    stadium_name singer_In_Concert.concert.stadium.name
    stadium_capacity singer_In_Concert.concert.stadium.capacity
    stadium_highest singer_In_Concert.concert.stadium.highest
    stadium_lowest singer_In_Concert.concert.stadium.lowest
    stadium_average singer_In_Concert.concert.stadium.average
    singer_id singer_In_Concert.singer.singer_id
    singer_name singer_In_Concert.singer.name
    singer_country singer_In_Concert.singer.country
    singer_song_name singer_In_Concert.singer.song_name
    singer_song_release_year singer_In_Concert.singer.song_release_year
    singer_age singer_In_Concert.singer.age
    singer_is_male singer_In_Concert.singer.is_male
    concert_id singer_In_Concert.concert.concert_id
    concert_name singer_In_Concert.concert.concert_name
    concert_theme singer_In_Concert.concert.theme
    concert_year singer_In_Concert.concert.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
