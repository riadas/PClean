using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("artist_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("artist_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["artist id", "exhibition id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "year"], Any[1, "theme"], Any[1, "ticket price"], Any[2, "date"], Any[2, "attendance"]]
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





PClean.@model ThemeGalleryModel begin
    @class Artist begin
        artist_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        year_join ~ ChooseUniformly(possibilities[:year_join])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Obs begin
        artist ~ Artist
        exhibition_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        theme ~ ChooseUniformly(possibilities[:theme])
        ticket_price ~ ChooseUniformly(possibilities[:ticket_price])
        date ~ ChooseUniformly(possibilities[:date])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end
end

query = @query ThemeGalleryModel.Obs [
    artist_id artist.artist_id
    artist_name artist.name
    artist_country artist.country
    artist_year_join artist.year_join
    artist_age artist.age
    exhibition_id exhibition_id
    exhibition_year year
    exhibition_theme theme
    exhibition_ticket_price ticket_price
    exhibition_record_date date
    exhibition_record_attendance attendance
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
