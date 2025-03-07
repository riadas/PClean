using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("singer_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("singer_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "singer id"], Any[0, "name"], Any[0, "birth year"], Any[0, "net worth millions"], Any[0, "citizenship"], Any[1, "song id"], Any[1, "title"], Any[1, "singer id"], Any[1, "sales"], Any[1, "highest position"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "singer id"], Any[0, "name"], Any[0, "birth year"], Any[0, "net worth millions"], Any[0, "citizenship"], Any[1, "song id"], Any[1, "title"], Any[1, "singer id"], Any[1, "sales"], Any[1, "highest position"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "singer id"], Any[0, "name"], Any[0, "birth year"], Any[0, "net worth millions"], Any[0, "citizenship"], Any[1, "song id"], Any[1, "title"], Any[1, "singer id"], Any[1, "sales"], Any[1, "highest position"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "singer id"], Any[0, "name"], Any[0, "birth year"], Any[0, "net worth millions"], Any[0, "citizenship"], Any[1, "song id"], Any[1, "title"], Any[1, "singer id"], Any[1, "sales"], Any[1, "highest position"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "singer id"], Any[0, "name"], Any[0, "birth year"], Any[0, "net worth millions"], Any[0, "citizenship"], Any[1, "song id"], Any[1, "title"], Any[1, "singer id"], Any[1, "sales"], Any[1, "highest position"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[8, 1]])
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







PClean.@model SingerModel begin
    @class Singer begin
        name ~ ChooseUniformly(possibilities[:name])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
        net_worth_millions ~ ChooseUniformly(possibilities[:net_worth_millions])
        citizenship ~ ChooseUniformly(possibilities[:citizenship])
    end

    @class Song begin
        title ~ ChooseUniformly(possibilities[:title])
        singer ~ Singer
        sales ~ ChooseUniformly(possibilities[:sales])
        highest_position ~ ChooseUniformly(possibilities[:highest_position])
    end

    @class Obs begin
        song ~ Song
    end
end

query = @query SingerModel.Obs [
    singer_id song.singer.singer_id
    singer_name song.singer.name
    singer_birth_year song.singer.birth_year
    singer_net_worth_millions song.singer.net_worth_millions
    singer_citizenship song.singer.citizenship
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
