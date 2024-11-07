using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("device_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("device_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "device id"], Any[0, "device"], Any[0, "carrier"], Any[0, "package version"], Any[0, "applications"], Any[0, "software platform"], Any[1, "shop id"], Any[1, "shop name"], Any[1, "location"], Any[1, "open date"], Any[1, "open year"], Any[2, "shop id"], Any[2, "device id"], Any[2, "quantity"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "device id"], Any[0, "device"], Any[0, "carrier"], Any[0, "package version"], Any[0, "applications"], Any[0, "software platform"], Any[1, "shop id"], Any[1, "shop name"], Any[1, "location"], Any[1, "open date"], Any[1, "open year"], Any[2, "shop id"], Any[2, "device id"], Any[2, "quantity"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model DeviceModel begin
    @class Device begin
        device_id ~ Unmodeled()
        device ~ ChooseUniformly(possibilities[:device])
        carrier ~ ChooseUniformly(possibilities[:carrier])
        package_version ~ ChooseUniformly(possibilities[:package_version])
        applications ~ ChooseUniformly(possibilities[:applications])
        software_platform ~ ChooseUniformly(possibilities[:software_platform])
    end

    @class Shop begin
        shop_id ~ Unmodeled()
        shop_name ~ ChooseUniformly(possibilities[:shop_name])
        location ~ ChooseUniformly(possibilities[:location])
        open_date ~ ChooseUniformly(possibilities[:open_date])
        open_year ~ ChooseUniformly(possibilities[:open_year])
    end

    @class Stock begin
        shop_id ~ Unmodeled()
        device_id ~ ChooseUniformly(possibilities[:device_id])
        quantity ~ ChooseUniformly(possibilities[:quantity])
    end

    @class Obs begin
        device ~ Device
        shop ~ Shop
        stock ~ Stock
    end
end

query = @query DeviceModel.Obs [
    device_id device.device_id
    device device.device
    device_carrier device.carrier
    device_package_version device.package_version
    device_applications device.applications
    device_software_platform device.software_platform
    shop_id shop.shop_id
    shop_name shop.shop_name
    shop_location shop.location
    shop_open_date shop.open_date
    shop_open_year shop.open_year
    stock_quantity stock.quantity
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
