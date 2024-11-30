using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "location id"], Any[1, "other details"], Any[2, "product id"], Any[2, "product type code"], Any[2, "product name"], Any[2, "product price"], Any[3, "party id"], Any[3, "party details"], Any[4, "asset id"], Any[4, "other details"], Any[5, "channel id"], Any[5, "other details"], Any[6, "finance id"], Any[6, "other details"], Any[7, "event id"], Any[7, "address id"], Any[7, "channel id"], Any[7, "event type code"], Any[7, "finance id"], Any[7, "location id"], Any[8, "product in event id"], Any[8, "event id"], Any[8, "product id"], Any[9, "party id"], Any[9, "event id"], Any[9, "role code"], Any[10, "document id"], Any[10, "event id"], Any[11, "asset id"], Any[11, "event id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "location id"], Any[1, "other details"], Any[2, "product id"], Any[2, "product type code"], Any[2, "product name"], Any[2, "product price"], Any[3, "party id"], Any[3, "party details"], Any[4, "asset id"], Any[4, "other details"], Any[5, "channel id"], Any[5, "other details"], Any[6, "finance id"], Any[6, "other details"], Any[7, "event id"], Any[7, "address id"], Any[7, "channel id"], Any[7, "event type code"], Any[7, "finance id"], Any[7, "location id"], Any[8, "product in event id"], Any[8, "event id"], Any[8, "product id"], Any[9, "party id"], Any[9, "event id"], Any[9, "role code"], Any[10, "document id"], Any[10, "event id"], Any[11, "asset id"], Any[11, "event id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["finance id", "address id", "location id", "product id", "event id", "event id", "party id", "event id", "event id", "event id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "address details"], Any[1, "other details"], Any[2, "product type code"], Any[2, "product name"], Any[2, "product price"], Any[3, "party details"], Any[4, "asset id"], Any[4, "other details"], Any[5, "channel id"], Any[5, "other details"], Any[6, "other details"], Any[7, "channel id"], Any[7, "event type code"], Any[8, "product in event id"], Any[9, "role code"], Any[10, "document id"], Any[11, "asset id"]]
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





PClean.@model SolvencyIiModel begin
    @class Addresses begin
        address_id ~ Unmodeled()
        address_details ~ ChooseUniformly(possibilities[:address_details])
    end

    @class Locations begin
        location_id ~ Unmodeled()
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Products begin
        product_id ~ Unmodeled()
        product_type_code ~ ChooseUniformly(possibilities[:product_type_code])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_price ~ ChooseUniformly(possibilities[:product_price])
    end

    @class Parties begin
        party_id ~ Unmodeled()
        party_details ~ ChooseUniformly(possibilities[:party_details])
    end

    @class Assets begin
        asset_id ~ Unmodeled()
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Channels begin
        channel_id ~ Unmodeled()
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Finances begin
        finance_id ~ Unmodeled()
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        addresses ~ Addresses
        locations ~ Locations
        products ~ Products
        parties ~ Parties
        assets ~ Assets
        channels ~ Channels
        finances ~ Finances
        event_id ~ Unmodeled()
        channel_id ~ ChooseUniformly(possibilities[:channel_id])
        event_type_code ~ ChooseUniformly(possibilities[:event_type_code])
        product_in_event_id ~ Unmodeled()
        role_code ~ ChooseUniformly(possibilities[:role_code])
        document_id ~ Unmodeled()
        asset_id ~ Unmodeled()
    end
end

query = @query SolvencyIiModel.Obs [
    addresses_address_id addresses.address_id
    addresses_address_details addresses.address_details
    locations_location_id locations.location_id
    locations_other_details locations.other_details
    products_product_id products.product_id
    products_product_type_code products.product_type_code
    products_product_name products.product_name
    products_product_price products.product_price
    parties_party_id parties.party_id
    parties_party_details parties.party_details
    assets_asset_id assets.asset_id
    assets_other_details assets.other_details
    channels_channel_id channels.channel_id
    channels_other_details channels.other_details
    finances_finance_id finances.finance_id
    finances_other_details finances.other_details
    events_event_id event_id
    events_channel_id channel_id
    events_event_type_code event_type_code
    products_in_events_product_in_event_id product_in_event_id
    parties_in_events_role_code role_code
    agreements_document_id document_id
    assets_in_events_asset_id asset_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
