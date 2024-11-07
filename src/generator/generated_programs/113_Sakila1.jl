using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("actor_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("actor_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "actor id"], Any[0, "first name"], Any[0, "last name"], Any[0, "last update"], Any[1, "address id"], Any[1, "address"], Any[1, "address2"], Any[1, "district"], Any[1, "city id"], Any[1, "postal code"], Any[1, "phone"], Any[1, "last update"], Any[2, "category id"], Any[2, "name"], Any[2, "last update"], Any[3, "city id"], Any[3, "city"], Any[3, "country id"], Any[3, "last update"], Any[4, "country id"], Any[4, "country"], Any[4, "last update"], Any[5, "customer id"], Any[5, "store id"], Any[5, "first name"], Any[5, "last name"], Any[5, "email"], Any[5, "address id"], Any[5, "active"], Any[5, "create date"], Any[5, "last update"], Any[6, "film id"], Any[6, "title"], Any[6, "description"], Any[6, "release year"], Any[6, "language id"], Any[6, "original language id"], Any[6, "rental duration"], Any[6, "rental rate"], Any[6, "length"], Any[6, "replacement cost"], Any[6, "rating"], Any[6, "special features"], Any[6, "last update"], Any[7, "actor id"], Any[7, "film id"], Any[7, "last update"], Any[8, "film id"], Any[8, "category id"], Any[8, "last update"], Any[9, "film id"], Any[9, "title"], Any[9, "description"], Any[10, "inventory id"], Any[10, "film id"], Any[10, "store id"], Any[10, "last update"], Any[11, "language id"], Any[11, "name"], Any[11, "last update"], Any[12, "payment id"], Any[12, "customer id"], Any[12, "staff id"], Any[12, "rental id"], Any[12, "amount"], Any[12, "payment date"], Any[12, "last update"], Any[13, "rental id"], Any[13, "rental date"], Any[13, "inventory id"], Any[13, "customer id"], Any[13, "return date"], Any[13, "staff id"], Any[13, "last update"], Any[14, "staff id"], Any[14, "first name"], Any[14, "last name"], Any[14, "address id"], Any[14, "picture"], Any[14, "email"], Any[14, "store id"], Any[14, "active"], Any[14, "username"], Any[14, "password"], Any[14, "last update"], Any[15, "store id"], Any[15, "manager staff id"], Any[15, "address id"], Any[15, "last update"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "actor id"], Any[0, "first name"], Any[0, "last name"], Any[0, "last update"], Any[1, "address id"], Any[1, "address"], Any[1, "address2"], Any[1, "district"], Any[1, "city id"], Any[1, "postal code"], Any[1, "phone"], Any[1, "last update"], Any[2, "category id"], Any[2, "name"], Any[2, "last update"], Any[3, "city id"], Any[3, "city"], Any[3, "country id"], Any[3, "last update"], Any[4, "country id"], Any[4, "country"], Any[4, "last update"], Any[5, "customer id"], Any[5, "store id"], Any[5, "first name"], Any[5, "last name"], Any[5, "email"], Any[5, "address id"], Any[5, "active"], Any[5, "create date"], Any[5, "last update"], Any[6, "film id"], Any[6, "title"], Any[6, "description"], Any[6, "release year"], Any[6, "language id"], Any[6, "original language id"], Any[6, "rental duration"], Any[6, "rental rate"], Any[6, "length"], Any[6, "replacement cost"], Any[6, "rating"], Any[6, "special features"], Any[6, "last update"], Any[7, "actor id"], Any[7, "film id"], Any[7, "last update"], Any[8, "film id"], Any[8, "category id"], Any[8, "last update"], Any[9, "film id"], Any[9, "title"], Any[9, "description"], Any[10, "inventory id"], Any[10, "film id"], Any[10, "store id"], Any[10, "last update"], Any[11, "language id"], Any[11, "name"], Any[11, "last update"], Any[12, "payment id"], Any[12, "customer id"], Any[12, "staff id"], Any[12, "rental id"], Any[12, "amount"], Any[12, "payment date"], Any[12, "last update"], Any[13, "rental id"], Any[13, "rental date"], Any[13, "inventory id"], Any[13, "customer id"], Any[13, "return date"], Any[13, "staff id"], Any[13, "last update"], Any[14, "staff id"], Any[14, "first name"], Any[14, "last name"], Any[14, "address id"], Any[14, "picture"], Any[14, "email"], Any[14, "store id"], Any[14, "active"], Any[14, "username"], Any[14, "password"], Any[14, "last update"], Any[15, "store id"], Any[15, "manager staff id"], Any[15, "address id"], Any[15, "last update"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Sakila1Model begin
    @class Actor begin
        actor_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Address begin
        address_id ~ Unmodeled()
        address ~ ChooseUniformly(possibilities[:address])
        address2 ~ ChooseUniformly(possibilities[:address2])
        district ~ ChooseUniformly(possibilities[:district])
        city_id ~ ChooseUniformly(possibilities[:city_id])
        postal_code ~ ChooseUniformly(possibilities[:postal_code])
        phone ~ ChooseUniformly(possibilities[:phone])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Category begin
        category_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class City begin
        city_id ~ Unmodeled()
        city ~ ChooseUniformly(possibilities[:city])
        country_id ~ ChooseUniformly(possibilities[:country_id])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Country begin
        country_id ~ Unmodeled()
        country ~ ChooseUniformly(possibilities[:country])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Customer begin
        customer_id ~ Unmodeled()
        store_id ~ ChooseUniformly(possibilities[:store_id])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        email ~ ChooseUniformly(possibilities[:email])
        address_id ~ ChooseUniformly(possibilities[:address_id])
        active ~ ChooseUniformly(possibilities[:active])
        create_date ~ TimePrior(possibilities[:create_date])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Film begin
        film_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        description ~ ChooseUniformly(possibilities[:description])
        release_year ~ TimePrior(possibilities[:release_year])
        language_id ~ ChooseUniformly(possibilities[:language_id])
        original_language_id ~ ChooseUniformly(possibilities[:original_language_id])
        rental_duration ~ ChooseUniformly(possibilities[:rental_duration])
        rental_rate ~ ChooseUniformly(possibilities[:rental_rate])
        length ~ ChooseUniformly(possibilities[:length])
        replacement_cost ~ ChooseUniformly(possibilities[:replacement_cost])
        rating ~ ChooseUniformly(possibilities[:rating])
        special_features ~ ChooseUniformly(possibilities[:special_features])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Film_Actor begin
        actor_id ~ Unmodeled()
        film_id ~ ChooseUniformly(possibilities[:film_id])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Film_Category begin
        film_id ~ Unmodeled()
        category_id ~ ChooseUniformly(possibilities[:category_id])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Film_Text begin
        film_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        description ~ ChooseUniformly(possibilities[:description])
    end

    @class Inventory begin
        inventory_id ~ Unmodeled()
        film_id ~ ChooseUniformly(possibilities[:film_id])
        store_id ~ ChooseUniformly(possibilities[:store_id])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Language begin
        language_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Payment begin
        payment_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        staff_id ~ ChooseUniformly(possibilities[:staff_id])
        rental_id ~ ChooseUniformly(possibilities[:rental_id])
        amount ~ ChooseUniformly(possibilities[:amount])
        payment_date ~ TimePrior(possibilities[:payment_date])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Rental begin
        rental_id ~ Unmodeled()
        rental_date ~ TimePrior(possibilities[:rental_date])
        inventory_id ~ ChooseUniformly(possibilities[:inventory_id])
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        return_date ~ TimePrior(possibilities[:return_date])
        staff_id ~ ChooseUniformly(possibilities[:staff_id])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Staff begin
        staff_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        address_id ~ ChooseUniformly(possibilities[:address_id])
        picture ~ ChooseUniformly(possibilities[:picture])
        email ~ ChooseUniformly(possibilities[:email])
        store_id ~ ChooseUniformly(possibilities[:store_id])
        active ~ ChooseUniformly(possibilities[:active])
        username ~ ChooseUniformly(possibilities[:username])
        password ~ ChooseUniformly(possibilities[:password])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Store begin
        store_id ~ Unmodeled()
        manager_staff_id ~ ChooseUniformly(possibilities[:manager_staff_id])
        address_id ~ ChooseUniformly(possibilities[:address_id])
        last_update ~ TimePrior(possibilities[:last_update])
    end

    @class Obs begin
        actor ~ Actor
        address ~ Address
        category ~ Category
        city ~ City
        country ~ Country
        customer ~ Customer
        film ~ Film
        film_Actor ~ Film_Actor
        film_Category ~ Film_Category
        film_Text ~ Film_Text
        inventory ~ Inventory
        language ~ Language
        payment ~ Payment
        rental ~ Rental
        staff ~ Staff
        store ~ Store
    end
end

query = @query Sakila1Model.Obs [
    actor_id actor.actor_id
    actor_first_name actor.first_name
    actor_last_name actor.last_name
    actor_last_update actor.last_update
    address_id address.address_id
    address address.address
    address2 address.address2
    address_district address.district
    address_postal_code address.postal_code
    address_phone address.phone
    address_last_update address.last_update
    category_id category.category_id
    category_name category.name
    category_last_update category.last_update
    city_id city.city_id
    city city.city
    city_last_update city.last_update
    country_id country.country_id
    country country.country
    country_last_update country.last_update
    customer_id customer.customer_id
    customer_first_name customer.first_name
    customer_last_name customer.last_name
    customer_email customer.email
    customer_active customer.active
    customer_create_date customer.create_date
    customer_last_update customer.last_update
    film_id film.film_id
    film_title film.title
    film_description film.description
    film_release_year film.release_year
    film_rental_duration film.rental_duration
    film_rental_rate film.rental_rate
    film_length film.length
    film_replacement_cost film.replacement_cost
    film_rating film.rating
    film_special_features film.special_features
    film_last_update film.last_update
    film_actor_last_update film_Actor.last_update
    film_category_last_update film_Category.last_update
    film_text_film_id film_Text.film_id
    film_text_title film_Text.title
    film_text_description film_Text.description
    inventory_id inventory.inventory_id
    inventory_last_update inventory.last_update
    language_id language.language_id
    language_name language.name
    language_last_update language.last_update
    payment_id payment.payment_id
    payment_amount payment.amount
    payment_date payment.payment_date
    payment_last_update payment.last_update
    rental_id rental.rental_id
    rental_date rental.rental_date
    rental_return_date rental.return_date
    rental_last_update rental.last_update
    staff_id staff.staff_id
    staff_first_name staff.first_name
    staff_last_name staff.last_name
    staff_picture staff.picture
    staff_email staff.email
    staff_store_id staff.store_id
    staff_active staff.active
    staff_username staff.username
    staff_password staff.password
    staff_last_update staff.last_update
    store_id store.store_id
    store_last_update store.last_update
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
