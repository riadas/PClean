using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("sqlite sequence_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("sqlite sequence_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "name"], Any[0, "seq"], Any[1, "id"], Any[1, "name"], Any[2, "id"], Any[2, "title"], Any[2, "artist id"], Any[3, "id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "id"], Any[4, "first name"], Any[4, "last name"], Any[4, "company"], Any[4, "address"], Any[4, "city"], Any[4, "state"], Any[4, "country"], Any[4, "postal code"], Any[4, "phone"], Any[4, "fax"], Any[4, "email"], Any[4, "support rep id"], Any[5, "id"], Any[5, "name"], Any[6, "id"], Any[6, "customer id"], Any[6, "invoice date"], Any[6, "billing address"], Any[6, "billing city"], Any[6, "billing state"], Any[6, "billing country"], Any[6, "billing postal code"], Any[6, "total"], Any[7, "id"], Any[7, "name"], Any[8, "id"], Any[8, "name"], Any[8, "album id"], Any[8, "media type id"], Any[8, "genre id"], Any[8, "composer"], Any[8, "milliseconds"], Any[8, "bytes"], Any[8, "unit price"], Any[9, "id"], Any[9, "invoice id"], Any[9, "track id"], Any[9, "unit price"], Any[9, "quantity"], Any[10, "id"], Any[10, "name"], Any[11, "playlist id"], Any[11, "track id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "name"], Any[0, "seq"], Any[1, "id"], Any[1, "name"], Any[2, "id"], Any[2, "title"], Any[2, "artist id"], Any[3, "id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "id"], Any[4, "first name"], Any[4, "last name"], Any[4, "company"], Any[4, "address"], Any[4, "city"], Any[4, "state"], Any[4, "country"], Any[4, "postal code"], Any[4, "phone"], Any[4, "fax"], Any[4, "email"], Any[4, "support rep id"], Any[5, "id"], Any[5, "name"], Any[6, "id"], Any[6, "customer id"], Any[6, "invoice date"], Any[6, "billing address"], Any[6, "billing city"], Any[6, "billing state"], Any[6, "billing country"], Any[6, "billing postal code"], Any[6, "total"], Any[7, "id"], Any[7, "name"], Any[8, "id"], Any[8, "name"], Any[8, "album id"], Any[8, "media type id"], Any[8, "genre id"], Any[8, "composer"], Any[8, "milliseconds"], Any[8, "bytes"], Any[8, "unit price"], Any[9, "id"], Any[9, "invoice id"], Any[9, "track id"], Any[9, "unit price"], Any[9, "quantity"], Any[10, "id"], Any[10, "name"], Any[11, "playlist id"], Any[11, "track id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Store1Model begin
    @class Sqlite_Sequence begin
        name ~ ChooseUniformly(possibilities[:name])
        seq ~ ChooseUniformly(possibilities[:seq])
    end

    @class Artists begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Albums begin
        id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        artist_id ~ ChooseUniformly(possibilities[:artist_id])
    end

    @class Employees begin
        id ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        title ~ ChooseUniformly(possibilities[:title])
        reports_to ~ ChooseUniformly(possibilities[:reports_to])
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
    end

    @class Customers begin
        id ~ Unmodeled()
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
        support_rep_id ~ ChooseUniformly(possibilities[:support_rep_id])
    end

    @class Genres begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Invoices begin
        id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        invoice_date ~ TimePrior(possibilities[:invoice_date])
        billing_address ~ ChooseUniformly(possibilities[:billing_address])
        billing_city ~ ChooseUniformly(possibilities[:billing_city])
        billing_state ~ ChooseUniformly(possibilities[:billing_state])
        billing_country ~ ChooseUniformly(possibilities[:billing_country])
        billing_postal_code ~ ChooseUniformly(possibilities[:billing_postal_code])
        total ~ ChooseUniformly(possibilities[:total])
    end

    @class Media_Types begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Tracks begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        album_id ~ ChooseUniformly(possibilities[:album_id])
        media_type_id ~ ChooseUniformly(possibilities[:media_type_id])
        genre_id ~ ChooseUniformly(possibilities[:genre_id])
        composer ~ ChooseUniformly(possibilities[:composer])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
        bytes ~ ChooseUniformly(possibilities[:bytes])
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
    end

    @class Invoice_Lines begin
        id ~ Unmodeled()
        invoice_id ~ ChooseUniformly(possibilities[:invoice_id])
        track_id ~ ChooseUniformly(possibilities[:track_id])
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
        quantity ~ ChooseUniformly(possibilities[:quantity])
    end

    @class Playlists begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Playlist_Tracks begin
        playlist_id ~ Unmodeled()
        track_id ~ ChooseUniformly(possibilities[:track_id])
    end

    @class Obs begin
        sqlite_Sequence ~ Sqlite_Sequence
        artists ~ Artists
        albums ~ Albums
        employees ~ Employees
        customers ~ Customers
        genres ~ Genres
        invoices ~ Invoices
        media_Types ~ Media_Types
        tracks ~ Tracks
        invoice_Lines ~ Invoice_Lines
        playlists ~ Playlists
        playlist_Tracks ~ Playlist_Tracks
    end
end

query = @query Store1Model.Obs [
    sqlite_sequence_name sqlite_Sequence.name
    sqlite_sequence_seq sqlite_Sequence.seq
    artists_id artists.id
    artists_name artists.name
    albums_id albums.id
    albums_title albums.title
    employees_id employees.id
    employees_last_name employees.last_name
    employees_first_name employees.first_name
    employees_title employees.title
    employees_birth_date employees.birth_date
    employees_hire_date employees.hire_date
    employees_address employees.address
    employees_city employees.city
    employees_state employees.state
    employees_country employees.country
    employees_postal_code employees.postal_code
    employees_phone employees.phone
    employees_fax employees.fax
    employees_email employees.email
    customers_id customers.id
    customers_first_name customers.first_name
    customers_last_name customers.last_name
    customers_company customers.company
    customers_address customers.address
    customers_city customers.city
    customers_state customers.state
    customers_country customers.country
    customers_postal_code customers.postal_code
    customers_phone customers.phone
    customers_fax customers.fax
    customers_email customers.email
    genres_id genres.id
    genres_name genres.name
    invoices_id invoices.id
    invoices_invoice_date invoices.invoice_date
    invoices_billing_address invoices.billing_address
    invoices_billing_city invoices.billing_city
    invoices_billing_state invoices.billing_state
    invoices_billing_country invoices.billing_country
    invoices_billing_postal_code invoices.billing_postal_code
    invoices_total invoices.total
    media_types_id media_Types.id
    media_types_name media_Types.name
    tracks_id tracks.id
    tracks_name tracks.name
    tracks_composer tracks.composer
    tracks_milliseconds tracks.milliseconds
    tracks_bytes tracks.bytes
    tracks_unit_price tracks.unit_price
    invoice_lines_id invoice_Lines.id
    invoice_lines_unit_price invoice_Lines.unit_price
    invoice_lines_quantity invoice_Lines.quantity
    playlists_id playlists.id
    playlists_name playlists.name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
