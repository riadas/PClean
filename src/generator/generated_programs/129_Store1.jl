using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("sqlite sequence_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("sqlite sequence_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "name"], Any[0, "seq"], Any[1, "id"], Any[1, "name"], Any[2, "id"], Any[2, "title"], Any[2, "artist id"], Any[3, "id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "id"], Any[4, "first name"], Any[4, "last name"], Any[4, "company"], Any[4, "address"], Any[4, "city"], Any[4, "state"], Any[4, "country"], Any[4, "postal code"], Any[4, "phone"], Any[4, "fax"], Any[4, "email"], Any[4, "support rep id"], Any[5, "id"], Any[5, "name"], Any[6, "id"], Any[6, "customer id"], Any[6, "invoice date"], Any[6, "billing address"], Any[6, "billing city"], Any[6, "billing state"], Any[6, "billing country"], Any[6, "billing postal code"], Any[6, "total"], Any[7, "id"], Any[7, "name"], Any[8, "id"], Any[8, "name"], Any[8, "album id"], Any[8, "media type id"], Any[8, "genre id"], Any[8, "composer"], Any[8, "milliseconds"], Any[8, "bytes"], Any[8, "unit price"], Any[9, "id"], Any[9, "invoice id"], Any[9, "track id"], Any[9, "unit price"], Any[9, "quantity"], Any[10, "id"], Any[10, "name"], Any[11, "playlist id"], Any[11, "track id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "name"], Any[0, "seq"], Any[1, "id"], Any[1, "name"], Any[2, "id"], Any[2, "title"], Any[2, "artist id"], Any[3, "id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "reports to"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "id"], Any[4, "first name"], Any[4, "last name"], Any[4, "company"], Any[4, "address"], Any[4, "city"], Any[4, "state"], Any[4, "country"], Any[4, "postal code"], Any[4, "phone"], Any[4, "fax"], Any[4, "email"], Any[4, "support rep id"], Any[5, "id"], Any[5, "name"], Any[6, "id"], Any[6, "customer id"], Any[6, "invoice date"], Any[6, "billing address"], Any[6, "billing city"], Any[6, "billing state"], Any[6, "billing country"], Any[6, "billing postal code"], Any[6, "total"], Any[7, "id"], Any[7, "name"], Any[8, "id"], Any[8, "name"], Any[8, "album id"], Any[8, "media type id"], Any[8, "genre id"], Any[8, "composer"], Any[8, "milliseconds"], Any[8, "bytes"], Any[8, "unit price"], Any[9, "id"], Any[9, "invoice id"], Any[9, "track id"], Any[9, "unit price"], Any[9, "quantity"], Any[10, "id"], Any[10, "name"], Any[11, "playlist id"], Any[11, "track id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["artist id", "reports to", "support rep id", "customer id", "media type id", "genre id", "album id", "track id", "invoice id", "track id", "playlist id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "seq"], Any[1, "id"], Any[1, "name"], Any[2, "id"], Any[2, "title"], Any[3, "id"], Any[3, "last name"], Any[3, "first name"], Any[3, "title"], Any[3, "birth date"], Any[3, "hire date"], Any[3, "address"], Any[3, "city"], Any[3, "state"], Any[3, "country"], Any[3, "postal code"], Any[3, "phone"], Any[3, "fax"], Any[3, "email"], Any[4, "id"], Any[4, "first name"], Any[4, "last name"], Any[4, "company"], Any[4, "address"], Any[4, "city"], Any[4, "state"], Any[4, "country"], Any[4, "postal code"], Any[4, "phone"], Any[4, "fax"], Any[4, "email"], Any[5, "id"], Any[5, "name"], Any[6, "id"], Any[6, "invoice date"], Any[6, "billing address"], Any[6, "billing city"], Any[6, "billing state"], Any[6, "billing country"], Any[6, "billing postal code"], Any[6, "total"], Any[7, "id"], Any[7, "name"], Any[8, "id"], Any[8, "name"], Any[8, "composer"], Any[8, "milliseconds"], Any[8, "bytes"], Any[8, "unit price"], Any[9, "id"], Any[9, "unit price"], Any[9, "quantity"], Any[10, "id"], Any[10, "name"]]
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





PClean.@model Store1Model begin
    @class Sqlite_Sequence begin
        name ~ ChooseUniformly(possibilities[:name])
        seq ~ ChooseUniformly(possibilities[:seq])
    end

    @class Artists begin
        id ~ ChooseUniformly(possibilities[:id])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Genres begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Media_Types begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Playlists begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Obs begin
        sqlite_Sequence ~ Sqlite_Sequence
        artists ~ Artists
        genres ~ Genres
        media_Types ~ Media_Types
        playlists ~ Playlists
        id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        id ~ Unmodeled()
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
        id ~ Unmodeled()
        invoice_date ~ TimePrior(possibilities[:invoice_date])
        billing_address ~ ChooseUniformly(possibilities[:billing_address])
        billing_city ~ ChooseUniformly(possibilities[:billing_city])
        billing_state ~ ChooseUniformly(possibilities[:billing_state])
        billing_country ~ ChooseUniformly(possibilities[:billing_country])
        billing_postal_code ~ ChooseUniformly(possibilities[:billing_postal_code])
        total ~ ChooseUniformly(possibilities[:total])
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        composer ~ ChooseUniformly(possibilities[:composer])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
        bytes ~ ChooseUniformly(possibilities[:bytes])
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
        id ~ Unmodeled()
        unit_price ~ ChooseUniformly(possibilities[:unit_price])
        quantity ~ ChooseUniformly(possibilities[:quantity])
    end
end

query = @query Store1Model.Obs [
    sqlite_sequence_name sqlite_Sequence.name
    sqlite_sequence_seq sqlite_Sequence.seq
    artists_id artists.id
    artists_name artists.name
    albums_id id
    albums_title title
    employees_id id
    employees_last_name last_name
    employees_first_name first_name
    employees_title title
    employees_birth_date birth_date
    employees_hire_date hire_date
    employees_address address
    employees_city city
    employees_state state
    employees_country country
    employees_postal_code postal_code
    employees_phone phone
    employees_fax fax
    employees_email email
    customers_id id
    customers_first_name first_name
    customers_last_name last_name
    customers_company company
    customers_address address
    customers_city city
    customers_state state
    customers_country country
    customers_postal_code postal_code
    customers_phone phone
    customers_fax fax
    customers_email email
    genres_id genres.id
    genres_name genres.name
    invoices_id id
    invoices_invoice_date invoice_date
    invoices_billing_address billing_address
    invoices_billing_city billing_city
    invoices_billing_state billing_state
    invoices_billing_country billing_country
    invoices_billing_postal_code billing_postal_code
    invoices_total total
    media_types_id media_Types.id
    media_types_name media_Types.name
    tracks_id id
    tracks_name name
    tracks_composer composer
    tracks_milliseconds milliseconds
    tracks_bytes bytes
    tracks_unit_price unit_price
    invoice_lines_id id
    invoice_lines_unit_price unit_price
    invoice_lines_quantity quantity
    playlists_id playlists.id
    playlists_name playlists.name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
