using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("album_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("album_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "album id"], Any[0, "title"], Any[0, "artist id"], Any[1, "artist id"], Any[1, "name"], Any[2, "customer id"], Any[2, "first name"], Any[2, "last name"], Any[2, "company"], Any[2, "address"], Any[2, "city"], Any[2, "state"], Any[2, "country"], Any[2, "postal code"], Any[2, "phone"], Any[2, "fax"], Any[2, "email"], Any[2, "support representative id"], Any[3, "employee id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "genre id"], Any[4, "name"], Any[5, "invoice id"], Any[5, "customer id"], Any[5, "invoice date"], Any[5, "billing address"], Any[5, "billing city"], Any[5, "billing state"], Any[5, "billing country"], Any[5, "billing postal code"], Any[5, "total"], Any[6, "invoice line id"], Any[6, "invoice id"], Any[6, "track id"], Any[6, "unit price"], Any[6, "quantity"], Any[7, "media type id"], Any[7, "name"], Any[8, "play list id"], Any[8, "name"], Any[9, "play list id"], Any[9, "track id"], Any[10, "track id"], Any[10, "name"], Any[10, "album id"], Any[10, "media type id"], Any[10, "genre id"], Any[10, "composer"], Any[10, "milliseconds"], Any[10, "bytes"], Any[10, "unit price"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "album id"], Any[0, "title"], Any[0, "artist id"], Any[1, "artist id"], Any[1, "name"], Any[2, "customer id"], Any[2, "first name"], Any[2, "last name"], Any[2, "company"], Any[2, "address"], Any[2, "city"], Any[2, "state"], Any[2, "country"], Any[2, "postal code"], Any[2, "phone"], Any[2, "fax"], Any[2, "email"], Any[2, "support representative id"], Any[3, "employee id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "genre id"], Any[4, "name"], Any[5, "invoice id"], Any[5, "customer id"], Any[5, "invoice date"], Any[5, "billing address"], Any[5, "billing city"], Any[5, "billing state"], Any[5, "billing country"], Any[5, "billing postal code"], Any[5, "total"], Any[6, "invoice line id"], Any[6, "invoice id"], Any[6, "track id"], Any[6, "unit price"], Any[6, "quantity"], Any[7, "media type id"], Any[7, "name"], Any[8, "play list id"], Any[8, "name"], Any[9, "play list id"], Any[9, "track id"], Any[10, "track id"], Any[10, "name"], Any[10, "album id"], Any[10, "media type id"], Any[10, "genre id"], Any[10, "composer"], Any[10, "milliseconds"], Any[10, "bytes"], Any[10, "unit price"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["artist id", "support representative id", "reports to", "customer id", "track id", "invoice id", "track id", "play list id", "media type id", "genre id", "album id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "title"], Any[1, "name"], Any[2, "first name"], Any[2, "last name"], Any[2, "company"], Any[2, "address"], Any[2, "city"], Any[2, "state"], Any[2, "country"], Any[2, "postal code"], Any[2, "phone"], Any[2, "fax"], Any[2, "email"], Any[3, "employee id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "name"], Any[5, "invoice date"], Any[5, "billing address"], Any[5, "billing city"], Any[5, "billing state"], Any[5, "billing country"], Any[5, "billing postal code"], Any[5, "total"], Any[6, "invoice line id"], Any[6, "unit price"], Any[6, "quantity"], Any[7, "name"], Any[8, "name"], Any[10, "name"], Any[10, "composer"], Any[10, "milliseconds"], Any[10, "bytes"], Any[10, "unit price"]]
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





PClean.@model Chinook1Model begin
    @class Artist begin
        artist_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Genre begin
        genre_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Media_Type begin
        media_type_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Playlist begin
        play_list_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Obs begin
        artist ~ Artist
        genre ~ Genre
        media_Type ~ Media_Type
        playlist ~ Playlist
        album_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        customer_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        company ~ ChooseUniformly(possibilities[:company])
        address ~ ChooseUniformly(possibilities[:address])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
        country ~ ChooseUniformly(possibilities[:country])
        postal_code ~ ChooseUniformly(possibilities[:postal_code])
        phone ~ ChooseUniformly(possibilities[:phone])
        fax ~ ChooseUniformly(possibilities[:fax])
        email ~ ChooseUniformly(possibilities[:email])
        employee_id ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        title ~ ChooseUniformly(possibilities[:title])
        birth_date ~ TimePrior(possibilities[:birth_date])
        hire_date ~ TimePrior(possibilities[:hire_date])
        address ~ ChooseUniformly(possibilities[:address])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
        country ~ ChooseUniformly(possibilities[:country])
        postal_code ~ ChooseUniformly(possibilities[:postal_code])
        phone ~ ChooseUniformly(possibilities[:phone])
        fax ~ ChooseUniformly(possibilities[:fax])
        email ~ ChooseUniformly(possibilities[:email])
        invoice_id ~ Unmodeled()
        invoice_date ~ TimePrior(possibilities[:invoice_date])
        billing_address ~ ChooseUniformly(possibilities[:billing_address])
        billing_city ~ ChooseUniformly(possibilities[:billing_city])
        billing_state ~ ChooseUniformly(possibilities[:billing_state])
        billing_country ~ ChooseUniformly(possibilities[:billing_country])
        billing_postal_code ~ ChooseUniformly(possibilities[:billing_postal_code])
        total ~ ChooseUniformly(possibilities[:total])
        invoice_line_id ~ Unmodeled()
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
        quantity ~ ChooseUniformly(possibilities[:quantity])
        track_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        composer ~ ChooseUniformly(possibilities[:composer])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
        bytes ~ ChooseUniformly(possibilities[:bytes])
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
    end
end

query = @query Chinook1Model.Obs [
    album_id album_id
    album_title title
    artist_id artist.artist_id
    artist_name artist.name
    customer_id customer_id
    customer_first_name first_name
    customer_last_name last_name
    customer_company company
    customer_address address
    customer_city city
    customer_state state
    customer_country country
    customer_postal_code postal_code
    customer_phone phone
    customer_fax fax
    customer_email email
    employee_id employee_id
    employee_last_name last_name
    employee_first_name first_name
    employee_title title
    employee_birth_date birth_date
    employee_hire_date hire_date
    employee_address address
    employee_city city
    employee_state state
    employee_country country
    employee_postal_code postal_code
    employee_phone phone
    employee_fax fax
    employee_email email
    genre_id genre.genre_id
    genre_name genre.name
    invoice_id invoice_id
    invoice_date invoice_date
    invoice_billing_address billing_address
    invoice_billing_city billing_city
    invoice_billing_state billing_state
    invoice_billing_country billing_country
    invoice_billing_postal_code billing_postal_code
    invoice_total total
    invoice_line_id invoice_line_id
    invoice_line_unit_price unit_price
    invoice_line_quantity quantity
    media_type_id media_Type.media_type_id
    media_type_name media_Type.name
    playlist_play_list_id playlist.play_list_id
    playlist_name playlist.name
    track_id track_id
    track_name name
    track_composer composer
    track_milliseconds milliseconds
    track_bytes bytes
    track_unit_price unit_price
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
