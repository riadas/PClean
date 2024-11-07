using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("album_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("album_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "album id"], Any[0, "title"], Any[0, "artist id"], Any[1, "artist id"], Any[1, "name"], Any[2, "customer id"], Any[2, "first name"], Any[2, "last name"], Any[2, "company"], Any[2, "address"], Any[2, "city"], Any[2, "state"], Any[2, "country"], Any[2, "postal code"], Any[2, "phone"], Any[2, "fax"], Any[2, "email"], Any[2, "support representative id"], Any[3, "employee id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "genre id"], Any[4, "name"], Any[5, "invoice id"], Any[5, "customer id"], Any[5, "invoice date"], Any[5, "billing address"], Any[5, "billing city"], Any[5, "billing state"], Any[5, "billing country"], Any[5, "billing postal code"], Any[5, "total"], Any[6, "invoice line id"], Any[6, "invoice id"], Any[6, "track id"], Any[6, "unit price"], Any[6, "quantity"], Any[7, "media type id"], Any[7, "name"], Any[8, "play list id"], Any[8, "name"], Any[9, "play list id"], Any[9, "track id"], Any[10, "track id"], Any[10, "name"], Any[10, "album id"], Any[10, "media type id"], Any[10, "genre id"], Any[10, "composer"], Any[10, "milliseconds"], Any[10, "bytes"], Any[10, "unit price"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "album id"], Any[0, "title"], Any[0, "artist id"], Any[1, "artist id"], Any[1, "name"], Any[2, "customer id"], Any[2, "first name"], Any[2, "last name"], Any[2, "company"], Any[2, "address"], Any[2, "city"], Any[2, "state"], Any[2, "country"], Any[2, "postal code"], Any[2, "phone"], Any[2, "fax"], Any[2, "email"], Any[2, "support representative id"], Any[3, "employee id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "genre id"], Any[4, "name"], Any[5, "invoice id"], Any[5, "customer id"], Any[5, "invoice date"], Any[5, "billing address"], Any[5, "billing city"], Any[5, "billing state"], Any[5, "billing country"], Any[5, "billing postal code"], Any[5, "total"], Any[6, "invoice line id"], Any[6, "invoice id"], Any[6, "track id"], Any[6, "unit price"], Any[6, "quantity"], Any[7, "media type id"], Any[7, "name"], Any[8, "play list id"], Any[8, "name"], Any[9, "play list id"], Any[9, "track id"], Any[10, "track id"], Any[10, "name"], Any[10, "album id"], Any[10, "media type id"], Any[10, "genre id"], Any[10, "composer"], Any[10, "milliseconds"], Any[10, "bytes"], Any[10, "unit price"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Chinook1Model begin
    @class Album begin
        album_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        artist_id ~ ChooseUniformly(possibilities[:artist_id])
    end

    @class Artist begin
        artist_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Customer begin
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
        support_representative_id ~ ChooseUniformly(possibilities[:support_representative_id])
    end

    @class Employee begin
        employee_id ~ Unmodeled()
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

    @class Genre begin
        genre_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Invoice begin
        invoice_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        invoice_date ~ TimePrior(possibilities[:invoice_date])
        billing_address ~ ChooseUniformly(possibilities[:billing_address])
        billing_city ~ ChooseUniformly(possibilities[:billing_city])
        billing_state ~ ChooseUniformly(possibilities[:billing_state])
        billing_country ~ ChooseUniformly(possibilities[:billing_country])
        billing_postal_code ~ ChooseUniformly(possibilities[:billing_postal_code])
        total ~ ChooseUniformly(possibilities[:total])
    end

    @class Invoice_Line begin
        invoice_line_id ~ Unmodeled()
        invoice_id ~ ChooseUniformly(possibilities[:invoice_id])
        track_id ~ ChooseUniformly(possibilities[:track_id])
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
        quantity ~ ChooseUniformly(possibilities[:quantity])
    end

    @class Media_Type begin
        media_type_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Playlist begin
        play_list_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Playlist_Track begin
        play_list_id ~ Unmodeled()
        track_id ~ ChooseUniformly(possibilities[:track_id])
    end

    @class Track begin
        track_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        album_id ~ ChooseUniformly(possibilities[:album_id])
        media_type_id ~ ChooseUniformly(possibilities[:media_type_id])
        genre_id ~ ChooseUniformly(possibilities[:genre_id])
        composer ~ ChooseUniformly(possibilities[:composer])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
        bytes ~ ChooseUniformly(possibilities[:bytes])
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
    end

    @class Obs begin
        album ~ Album
        artist ~ Artist
        customer ~ Customer
        employee ~ Employee
        genre ~ Genre
        invoice ~ Invoice
        invoice_Line ~ Invoice_Line
        media_Type ~ Media_Type
        playlist ~ Playlist
        playlist_Track ~ Playlist_Track
        track ~ Track
    end
end

query = @query Chinook1Model.Obs [
    album_id album.album_id
    album_title album.title
    artist_id artist.artist_id
    artist_name artist.name
    customer_id customer.customer_id
    customer_first_name customer.first_name
    customer_last_name customer.last_name
    customer_company customer.company
    customer_address customer.address
    customer_city customer.city
    customer_state customer.state
    customer_country customer.country
    customer_postal_code customer.postal_code
    customer_phone customer.phone
    customer_fax customer.fax
    customer_email customer.email
    employee_id employee.employee_id
    employee_last_name employee.last_name
    employee_first_name employee.first_name
    employee_title employee.title
    employee_birth_date employee.birth_date
    employee_hire_date employee.hire_date
    employee_address employee.address
    employee_city employee.city
    employee_state employee.state
    employee_country employee.country
    employee_postal_code employee.postal_code
    employee_phone employee.phone
    employee_fax employee.fax
    employee_email employee.email
    genre_id genre.genre_id
    genre_name genre.name
    invoice_id invoice.invoice_id
    invoice_date invoice.invoice_date
    invoice_billing_address invoice.billing_address
    invoice_billing_city invoice.billing_city
    invoice_billing_state invoice.billing_state
    invoice_billing_country invoice.billing_country
    invoice_billing_postal_code invoice.billing_postal_code
    invoice_total invoice.total
    invoice_line_id invoice_Line.invoice_line_id
    invoice_line_unit_price invoice_Line.unit_price
    invoice_line_quantity invoice_Line.quantity
    media_type_id media_Type.media_type_id
    media_type_name media_Type.name
    playlist_play_list_id playlist.play_list_id
    playlist_name playlist.name
    track_id track.track_id
    track_name track.name
    track_composer track.composer
    track_milliseconds track.milliseconds
    track_bytes track.bytes
    track_unit_price track.unit_price
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
