using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("singer_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("singer_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "singer id"], Any[0, "name"], Any[0, "birth year"], Any[0, "net worth millions"], Any[0, "citizenship"], Any[1, "song id"], Any[1, "title"], Any[1, "singer id"], Any[1, "sales"], Any[1, "highest position"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "singer id"], Any[0, "name"], Any[0, "birth year"], Any[0, "net worth millions"], Any[0, "citizenship"], Any[1, "song id"], Any[1, "title"], Any[1, "singer id"], Any[1, "sales"], Any[1, "highest position"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model SingerModel begin
    @class Singer begin
        singer_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
        net_worth_millions ~ ChooseUniformly(possibilities[:net_worth_millions])
        citizenship ~ ChooseUniformly(possibilities[:citizenship])
    end

    @class Song begin
        song_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        singer_id ~ ChooseUniformly(possibilities[:singer_id])
        sales ~ ChooseUniformly(possibilities[:sales])
        highest_position ~ ChooseUniformly(possibilities[:highest_position])
    end

    @class Obs begin
        singer ~ Singer
        song ~ Song
    end
end

query = @query SingerModel.Obs [
    singer_id singer.singer_id
    singer_name singer.name
    singer_birth_year singer.birth_year
    singer_net_worth_millions singer.net_worth_millions
    singer_citizenship singer.citizenship
    song_id song.song_id
    song_title song.title
    song_sales song.sales
    song_highest_position song.highest_position
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
