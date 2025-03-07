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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "exhibition id"], Any[1, "year"], Any[1, "theme"], Any[1, "artist id"], Any[1, "ticket price"], Any[2, "exhibition id"], Any[2, "date"], Any[2, "attendance"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "exhibition id"], Any[1, "year"], Any[1, "theme"], Any[1, "artist id"], Any[1, "ticket price"], Any[2, "exhibition id"], Any[2, "date"], Any[2, "attendance"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "exhibition id"], Any[1, "year"], Any[1, "theme"], Any[1, "artist id"], Any[1, "ticket price"], Any[2, "exhibition id"], Any[2, "date"], Any[2, "attendance"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "exhibition id"], Any[1, "year"], Any[1, "theme"], Any[1, "artist id"], Any[1, "ticket price"], Any[2, "exhibition id"], Any[2, "date"], Any[2, "attendance"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "exhibition id"], Any[1, "year"], Any[1, "theme"], Any[1, "artist id"], Any[1, "ticket price"], Any[2, "exhibition id"], Any[2, "date"], Any[2, "attendance"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 1], Any[11, 6]])
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







PClean.@model ThemeGalleryModel begin
    @class Artist begin
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        year_join ~ ChooseUniformly(possibilities[:year_join])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Exhibition begin
        year ~ ChooseUniformly(possibilities[:year])
        theme ~ ChooseUniformly(possibilities[:theme])
        artist ~ Artist
        ticket_price ~ ChooseUniformly(possibilities[:ticket_price])
    end

    @class Exhibition_record begin
        date ~ ChooseUniformly(possibilities[:date])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end

    @class Obs begin
        exhibition_record ~ Exhibition_record
    end
end

query = @query ThemeGalleryModel.Obs [
    artist_id exhibition_record.exhibition.artist.artist_id
    artist_name exhibition_record.exhibition.artist.name
    artist_country exhibition_record.exhibition.artist.country
    artist_year_join exhibition_record.exhibition.artist.year_join
    artist_age exhibition_record.exhibition.artist.age
    exhibition_id exhibition_record.exhibition.exhibition_id
    exhibition_year exhibition_record.exhibition.year
    exhibition_theme exhibition_record.exhibition.theme
    exhibition_ticket_price exhibition_record.exhibition.ticket_price
    exhibition_record_date exhibition_record.date
    exhibition_record_attendance exhibition_record.attendance
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
