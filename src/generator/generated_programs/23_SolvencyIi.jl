using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "location id"], Any[1, "other details"], Any[2, "product id"], Any[2, "product type code"], Any[2, "product name"], Any[2, "product price"], Any[3, "party id"], Any[3, "party details"], Any[4, "asset id"], Any[4, "other details"], Any[5, "channel id"], Any[5, "other details"], Any[6, "finance id"], Any[6, "other details"], Any[7, "event id"], Any[7, "address id"], Any[7, "channel id"], Any[7, "event type code"], Any[7, "finance id"], Any[7, "location id"], Any[8, "product in event id"], Any[8, "event id"], Any[8, "product id"], Any[9, "party id"], Any[9, "event id"], Any[9, "role code"], Any[10, "document id"], Any[10, "event id"], Any[11, "asset id"], Any[11, "event id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "address details"], Any[1, "location id"], Any[1, "other details"], Any[2, "product id"], Any[2, "product type code"], Any[2, "product name"], Any[2, "product price"], Any[3, "party id"], Any[3, "party details"], Any[4, "asset id"], Any[4, "other details"], Any[5, "channel id"], Any[5, "other details"], Any[6, "finance id"], Any[6, "other details"], Any[7, "event id"], Any[7, "address id"], Any[7, "channel id"], Any[7, "event type code"], Any[7, "finance id"], Any[7, "location id"], Any[8, "product in event id"], Any[8, "event id"], Any[8, "product id"], Any[9, "party id"], Any[9, "event id"], Any[9, "role code"], Any[10, "document id"], Any[10, "event id"], Any[11, "asset id"], Any[11, "event id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Events begin
        event_id ~ Unmodeled()
        address_id ~ ChooseUniformly(possibilities[:address_id])
        channel_id ~ ChooseUniformly(possibilities[:channel_id])
        event_type_code ~ ChooseUniformly(possibilities[:event_type_code])
        finance_id ~ ChooseUniformly(possibilities[:finance_id])
        location_id ~ ChooseUniformly(possibilities[:location_id])
    end

    @class Products_In_Events begin
        product_in_event_id ~ Unmodeled()
        event_id ~ ChooseUniformly(possibilities[:event_id])
        product_id ~ ChooseUniformly(possibilities[:product_id])
    end

    @class Parties_In_Events begin
        party_id ~ Unmodeled()
        event_id ~ ChooseUniformly(possibilities[:event_id])
        role_code ~ ChooseUniformly(possibilities[:role_code])
    end

    @class Agreements begin
        document_id ~ Unmodeled()
        event_id ~ ChooseUniformly(possibilities[:event_id])
    end

    @class Assets_In_Events begin
        asset_id ~ Unmodeled()
        event_id ~ ChooseUniformly(possibilities[:event_id])
    end

    @class Obs begin
        addresses ~ Addresses
        locations ~ Locations
        products ~ Products
        parties ~ Parties
        assets ~ Assets
        channels ~ Channels
        finances ~ Finances
        events ~ Events
        products_In_Events ~ Products_In_Events
        parties_In_Events ~ Parties_In_Events
        agreements ~ Agreements
        assets_In_Events ~ Assets_In_Events
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
    events_event_id events.event_id
    events_channel_id events.channel_id
    events_event_type_code events.event_type_code
    products_in_events_product_in_event_id products_In_Events.product_in_event_id
    parties_in_events_role_code parties_In_Events.role_code
    agreements_document_id agreements.document_id
    assets_in_events_asset_id assets_In_Events.asset_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
