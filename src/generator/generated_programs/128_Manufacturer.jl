using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("manufacturer_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("manufacturer_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "manufacturer id"], Any[0, "open year"], Any[0, "name"], Any[0, "num of factories"], Any[0, "num of shops"], Any[1, "furniture id"], Any[1, "name"], Any[1, "num of component"], Any[1, "market rate"], Any[2, "manufacturer id"], Any[2, "furniture id"], Any[2, "price in dollar"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "manufacturer id"], Any[0, "open year"], Any[0, "name"], Any[0, "num of factories"], Any[0, "num of shops"], Any[1, "furniture id"], Any[1, "name"], Any[1, "num of component"], Any[1, "market rate"], Any[2, "manufacturer id"], Any[2, "furniture id"], Any[2, "price in dollar"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ManufacturerModel begin
    @class Manufacturer begin
        manufacturer_id ~ Unmodeled()
        open_year ~ ChooseUniformly(possibilities[:open_year])
        name ~ ChooseUniformly(possibilities[:name])
        num_of_factories ~ ChooseUniformly(possibilities[:num_of_factories])
        num_of_shops ~ ChooseUniformly(possibilities[:num_of_shops])
    end

    @class Furniture begin
        furniture_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        num_of_component ~ ChooseUniformly(possibilities[:num_of_component])
        market_rate ~ ChooseUniformly(possibilities[:market_rate])
    end

    @class Furniture_Manufacte begin
        manufacturer_id ~ Unmodeled()
        furniture_id ~ ChooseUniformly(possibilities[:furniture_id])
        price_in_dollar ~ ChooseUniformly(possibilities[:price_in_dollar])
    end

    @class Obs begin
        manufacturer ~ Manufacturer
        furniture ~ Furniture
        furniture_Manufacte ~ Furniture_Manufacte
    end
end

query = @query ManufacturerModel.Obs [
    manufacturer_id manufacturer.manufacturer_id
    manufacturer_open_year manufacturer.open_year
    manufacturer_name manufacturer.name
    manufacturer_num_of_factories manufacturer.num_of_factories
    manufacturer_num_of_shops manufacturer.num_of_shops
    furniture_id furniture.furniture_id
    furniture_name furniture.name
    furniture_num_of_component furniture.num_of_component
    furniture_market_rate furniture.market_rate
    furniture_manufacte_price_in_dollar furniture_Manufacte.price_in_dollar
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
