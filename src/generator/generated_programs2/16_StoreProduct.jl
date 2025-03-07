using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("product_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("product_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product"], Any[0, "dimensions"], Any[0, "dpi"], Any[0, "pages per minute color"], Any[0, "max page size"], Any[0, "interface"], Any[1, "store id"], Any[1, "store name"], Any[1, "type"], Any[1, "area size"], Any[1, "number of product category"], Any[1, "ranking"], Any[2, "district id"], Any[2, "district name"], Any[2, "headquartered city"], Any[2, "city population"], Any[2, "city area"], Any[3, "store id"], Any[3, "product id"], Any[4, "store id"], Any[4, "district id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product"], Any[0, "dimensions"], Any[0, "dpi"], Any[0, "pages per minute color"], Any[0, "max page size"], Any[0, "interface"], Any[1, "store id"], Any[1, "store name"], Any[1, "type"], Any[1, "area size"], Any[1, "number of product category"], Any[1, "ranking"], Any[2, "district id"], Any[2, "district name"], Any[2, "headquartered city"], Any[2, "city population"], Any[2, "city area"], Any[3, "store id"], Any[3, "product id"], Any[4, "store id"], Any[4, "district id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product"], Any[0, "dimensions"], Any[0, "dpi"], Any[0, "pages per minute color"], Any[0, "max page size"], Any[0, "interface"], Any[1, "store id"], Any[1, "store name"], Any[1, "type"], Any[1, "area size"], Any[1, "number of product category"], Any[1, "ranking"], Any[2, "district id"], Any[2, "district name"], Any[2, "headquartered city"], Any[2, "city population"], Any[2, "city area"], Any[3, "store id"], Any[3, "product id"], Any[4, "store id"], Any[4, "district id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product"], Any[0, "dimensions"], Any[0, "dpi"], Any[0, "pages per minute color"], Any[0, "max page size"], Any[0, "interface"], Any[1, "store id"], Any[1, "store name"], Any[1, "type"], Any[1, "area size"], Any[1, "number of product category"], Any[1, "ranking"], Any[2, "district id"], Any[2, "district name"], Any[2, "headquartered city"], Any[2, "city population"], Any[2, "city area"], Any[3, "store id"], Any[3, "product id"], Any[4, "store id"], Any[4, "district id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "product id"], Any[0, "product"], Any[0, "dimensions"], Any[0, "dpi"], Any[0, "pages per minute color"], Any[0, "max page size"], Any[0, "interface"], Any[1, "store id"], Any[1, "store name"], Any[1, "type"], Any[1, "area size"], Any[1, "number of product category"], Any[1, "ranking"], Any[2, "district id"], Any[2, "district name"], Any[2, "headquartered city"], Any[2, "city population"], Any[2, "city area"], Any[3, "store id"], Any[3, "product id"], Any[4, "store id"], Any[4, "district id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[19, 8], Any[22, 14], Any[21, 8]])
column_names_without_foreign_keys = filter(tup -> !(tup in foreign_keys), cols)
matching_columns = []
for col in dirty_columns 
    println(col)
    match_indices = findall(tup -> lowercase(join(split(join(split(tup[2], " "), ""), "_"), "")) == lowercase(join(split(join(split(col, " "), ""), "_"), "")), column_names_without_foreign_keys)
    if length(match_indices) > 0
        push!(matching_columns, column_names_without_foreign_keys[match_indices[1]][2])
    else
        error("matching column not found")
    end
end
column_renaming_dict = Dict(zip(dirty_columns, matching_columns))
column_renaming_dict_reverse = Dict(zip(matching_columns, dirty_columns))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in dirty_columns
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))







PClean.@model StoreProductModel begin
    @class Product begin
        product ~ ChooseUniformly(possibilities[:product])
        dimensions ~ ChooseUniformly(possibilities[:dimensions])
        dpi ~ ChooseUniformly(possibilities[:dpi])
        pages_per_minute_color ~ ChooseUniformly(possibilities[:pages_per_minute_color])
        max_page_size ~ ChooseUniformly(possibilities[:max_page_size])
        interface ~ ChooseUniformly(possibilities[:interface])
    end

    @class Store begin
        store_name ~ ChooseUniformly(possibilities[:store_name])
        type ~ ChooseUniformly(possibilities[:type])
        area_size ~ ChooseUniformly(possibilities[:area_size])
        number_of_product_category ~ ChooseUniformly(possibilities[:number_of_product_category])
        ranking ~ ChooseUniformly(possibilities[:ranking])
    end

    @class District begin
        district_name ~ ChooseUniformly(possibilities[:district_name])
        headquartered_city ~ ChooseUniformly(possibilities[:headquartered_city])
        city_population ~ ChooseUniformly(possibilities[:city_population])
        city_area ~ ChooseUniformly(possibilities[:city_area])
    end

    @class Store_product begin
        product_id ~ Unmodeled()
    end

    @class Store_district begin
        district ~ District
    end

    @class Obs begin
        product ~ Product
        store_product ~ Store_product
        store_district ~ Store_district
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
    store_id store_product.store.store_id
    store_name store_product.store.store_name
    store_type store_product.store.type
    store_area_size store_product.store.area_size
    store_number_of_product_category store_product.store.number_of_product_category
    store_ranking store_product.store.ranking
    district_id store_district.district.district_id
    district_name store_district.district.district_name
    district_headquartered_city store_district.district.headquartered_city
    district_city_population store_district.district.city_population
    district_city_area store_district.district.city_area
    store_product_product_id store_product.product_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
