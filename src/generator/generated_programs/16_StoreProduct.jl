using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("product_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("product_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product"], Any[0, "dimensions"], Any[0, "dpi"], Any[0, "pages per minute color"], Any[0, "max page size"], Any[0, "interface"], Any[1, "store id"], Any[1, "store name"], Any[1, "type"], Any[1, "area size"], Any[1, "number of product category"], Any[1, "ranking"], Any[2, "district id"], Any[2, "district name"], Any[2, "headquartered city"], Any[2, "city population"], Any[2, "city area"], Any[3, "store id"], Any[3, "product id"], Any[4, "store id"], Any[4, "district id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product"], Any[0, "dimensions"], Any[0, "dpi"], Any[0, "pages per minute color"], Any[0, "max page size"], Any[0, "interface"], Any[1, "store id"], Any[1, "store name"], Any[1, "type"], Any[1, "area size"], Any[1, "number of product category"], Any[1, "ranking"], Any[2, "district id"], Any[2, "district name"], Any[2, "headquartered city"], Any[2, "city population"], Any[2, "city area"], Any[3, "store id"], Any[3, "product id"], Any[4, "store id"], Any[4, "district id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model StoreProductModel begin
    @class Product begin
        product_id ~ Unmodeled()
        product ~ ChooseUniformly(possibilities[:product])
        dimensions ~ ChooseUniformly(possibilities[:dimensions])
        dpi ~ ChooseUniformly(possibilities[:dpi])
        pages_per_minute_color ~ ChooseUniformly(possibilities[:pages_per_minute_color])
        max_page_size ~ ChooseUniformly(possibilities[:max_page_size])
        interface ~ ChooseUniformly(possibilities[:interface])
    end

    @class Store begin
        store_id ~ Unmodeled()
        store_name ~ ChooseUniformly(possibilities[:store_name])
        type ~ ChooseUniformly(possibilities[:type])
        area_size ~ ChooseUniformly(possibilities[:area_size])
        number_of_product_category ~ ChooseUniformly(possibilities[:number_of_product_category])
        ranking ~ ChooseUniformly(possibilities[:ranking])
    end

    @class District begin
        district_id ~ Unmodeled()
        district_name ~ ChooseUniformly(possibilities[:district_name])
        headquartered_city ~ ChooseUniformly(possibilities[:headquartered_city])
        city_population ~ ChooseUniformly(possibilities[:city_population])
        city_area ~ ChooseUniformly(possibilities[:city_area])
    end

    @class Store_Product begin
        store_id ~ Unmodeled()
        product_id ~ ChooseUniformly(possibilities[:product_id])
    end

    @class Store_District begin
        store_id ~ Unmodeled()
        district_id ~ ChooseUniformly(possibilities[:district_id])
    end

    @class Obs begin
        product ~ Product
        store ~ Store
        district ~ District
        store_Product ~ Store_Product
        store_District ~ Store_District
    end
end

query = @query StoreProductModel.Obs [
    product_id product.product_id
    product product.product
    product_dimensions product.dimensions
    product_dpi product.dpi
    product_pages_per_minute_color product.pages_per_minute_color
    product_max_page_size product.max_page_size
    product_interface product.interface
    store_id store.store_id
    store_name store.store_name
    store_type store.type
    store_area_size store.area_size
    store_number_of_product_category store.number_of_product_category
    store_ranking store.ranking
    district_id district.district_id
    district_name district.district_name
    district_headquartered_city district.headquartered_city
    district_city_population district.city_population
    district_city_area district.city_area
    store_product_product_id store_Product.product_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
