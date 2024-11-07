using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("artist_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("artist_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "exhibition id"], Any[1, "year"], Any[1, "theme"], Any[1, "artist id"], Any[1, "ticket price"], Any[2, "exhibition id"], Any[2, "date"], Any[2, "attendance"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "artist id"], Any[0, "name"], Any[0, "country"], Any[0, "year join"], Any[0, "age"], Any[1, "exhibition id"], Any[1, "year"], Any[1, "theme"], Any[1, "artist id"], Any[1, "ticket price"], Any[2, "exhibition id"], Any[2, "date"], Any[2, "attendance"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Exhibition begin
        exhibition_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        theme ~ ChooseUniformly(possibilities[:theme])
        artist_id ~ ChooseUniformly(possibilities[:artist_id])
        ticket_price ~ ChooseUniformly(possibilities[:ticket_price])
    end

    @class Exhibition_Record begin
        exhibition_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end

    @class Obs begin
        artist ~ Artist
        exhibition ~ Exhibition
        exhibition_Record ~ Exhibition_Record
    end
end

query = @query ThemeGalleryModel.Obs [
    artist_id artist.artist_id
    artist_name artist.name
    artist_country artist.country
    artist_year_join artist.year_join
    artist_age artist.age
    exhibition_id exhibition.exhibition_id
    exhibition_year exhibition.year
    exhibition_theme exhibition.theme
    exhibition_ticket_price exhibition.ticket_price
    exhibition_record_date exhibition_Record.date
    exhibition_record_attendance exhibition_Record.attendance
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
