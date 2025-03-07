using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference_characteristic_types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference_characteristic_types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "characteristic type code"], Any[0, "characteristic type description"], Any[1, "color code"], Any[1, "color description"], Any[2, "product category code"], Any[2, "product category description"], Any[2, "unit of measure"], Any[3, "characteristic id"], Any[3, "characteristic type code"], Any[3, "characteristic data type"], Any[3, "characteristic name"], Any[3, "other characteristic details"], Any[4, "product id"], Any[4, "color code"], Any[4, "product category code"], Any[4, "product name"], Any[4, "typical buying price"], Any[4, "typical selling price"], Any[4, "product description"], Any[4, "other product details"], Any[5, "product id"], Any[5, "characteristic id"], Any[5, "product characteristic value"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "characteristic type code"], Any[0, "characteristic type description"], Any[1, "color code"], Any[1, "color description"], Any[2, "product category code"], Any[2, "product category description"], Any[2, "unit of measure"], Any[3, "characteristic id"], Any[3, "characteristic type code"], Any[3, "characteristic data type"], Any[3, "characteristic name"], Any[3, "other characteristic details"], Any[4, "product id"], Any[4, "color code"], Any[4, "product category code"], Any[4, "product name"], Any[4, "typical buying price"], Any[4, "typical selling price"], Any[4, "product description"], Any[4, "other product details"], Any[5, "product id"], Any[5, "characteristic id"], Any[5, "product characteristic value"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "characteristic type code"], Any[0, "characteristic type description"], Any[1, "color code"], Any[1, "color description"], Any[2, "product category code"], Any[2, "product category description"], Any[2, "unit of measure"], Any[3, "characteristic id"], Any[3, "characteristic type code"], Any[3, "characteristic data type"], Any[3, "characteristic name"], Any[3, "other characteristic details"], Any[4, "product id"], Any[4, "color code"], Any[4, "product category code"], Any[4, "product name"], Any[4, "typical buying price"], Any[4, "typical selling price"], Any[4, "product description"], Any[4, "other product details"], Any[5, "product id"], Any[5, "characteristic id"], Any[5, "product characteristic value"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "characteristic type code"], Any[0, "characteristic type description"], Any[1, "color code"], Any[1, "color description"], Any[2, "product category code"], Any[2, "product category description"], Any[2, "unit of measure"], Any[3, "characteristic id"], Any[3, "characteristic type code"], Any[3, "characteristic data type"], Any[3, "characteristic name"], Any[3, "other characteristic details"], Any[4, "product id"], Any[4, "color code"], Any[4, "product category code"], Any[4, "product name"], Any[4, "typical buying price"], Any[4, "typical selling price"], Any[4, "product description"], Any[4, "other product details"], Any[5, "product id"], Any[5, "characteristic id"], Any[5, "product characteristic value"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "characteristic type code"], Any[0, "characteristic type description"], Any[1, "color code"], Any[1, "color description"], Any[2, "product category code"], Any[2, "product category description"], Any[2, "unit of measure"], Any[3, "characteristic id"], Any[3, "characteristic type code"], Any[3, "characteristic data type"], Any[3, "characteristic name"], Any[3, "other characteristic details"], Any[4, "product id"], Any[4, "color code"], Any[4, "product category code"], Any[4, "product name"], Any[4, "typical buying price"], Any[4, "typical selling price"], Any[4, "product description"], Any[4, "other product details"], Any[5, "product id"], Any[5, "characteristic id"], Any[5, "product characteristic value"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 1], Any[14, 3], Any[15, 5], Any[21, 13], Any[22, 8]])
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







PClean.@model ProductsGenCharacteristicsModel begin
    @class Reference_characteristic_types begin
        characteristic_type_code ~ ChooseUniformly(possibilities[:characteristic_type_code])
        characteristic_type_description ~ ChooseUniformly(possibilities[:characteristic_type_description])
    end

    @class Reference_colors begin
        color_code ~ ChooseUniformly(possibilities[:color_code])
        color_description ~ ChooseUniformly(possibilities[:color_description])
    end

    @class Reference_product_categories begin
        product_category_code ~ ChooseUniformly(possibilities[:product_category_code])
        product_category_description ~ ChooseUniformly(possibilities[:product_category_description])
        unit_of_measure ~ ChooseUniformly(possibilities[:unit_of_measure])
    end

    @class Characteristics begin
        reference_characteristic_types ~ Reference_characteristic_types
        characteristic_data_type ~ ChooseUniformly(possibilities[:characteristic_data_type])
        characteristic_name ~ ChooseUniformly(possibilities[:characteristic_name])
        other_characteristic_details ~ ChooseUniformly(possibilities[:other_characteristic_details])
    end

    @class Products begin
        reference_colors ~ Reference_colors
        reference_product_categories ~ Reference_product_categories
        product_name ~ ChooseUniformly(possibilities[:product_name])
        typical_buying_price ~ ChooseUniformly(possibilities[:typical_buying_price])
        typical_selling_price ~ ChooseUniformly(possibilities[:typical_selling_price])
        product_description ~ ChooseUniformly(possibilities[:product_description])
        other_product_details ~ ChooseUniformly(possibilities[:other_product_details])
    end

    @class Product_characteristics begin
        products ~ Products
        characteristics ~ Characteristics
        product_characteristic_value ~ ChooseUniformly(possibilities[:product_characteristic_value])
    end

    @class Obs begin
        product_characteristics ~ Product_characteristics
    end
end

query = @query ProductsGenCharacteristicsModel.Obs [
    reference_characteristic_types_characteristic_type_code product_characteristics.characteristics.reference_characteristic_types.characteristic_type_code
    reference_characteristic_types_characteristic_type_description product_characteristics.characteristics.reference_characteristic_types.characteristic_type_description
    reference_colors_color_code product_characteristics.products.reference_colors.color_code
    reference_colors_color_description product_characteristics.products.reference_colors.color_description
    reference_product_categories_product_category_code product_characteristics.products.reference_product_categories.product_category_code
    reference_product_categories_product_category_description product_characteristics.products.reference_product_categories.product_category_description
    reference_product_categories_unit_of_measure product_characteristics.products.reference_product_categories.unit_of_measure
    characteristics_characteristic_id product_characteristics.characteristics.characteristic_id
    characteristics_characteristic_data_type product_characteristics.characteristics.characteristic_data_type
    characteristics_characteristic_name product_characteristics.characteristics.characteristic_name
    characteristics_other_characteristic_details product_characteristics.characteristics.other_characteristic_details
    products_product_id product_characteristics.products.product_id
    products_product_name product_characteristics.products.product_name
    products_typical_buying_price product_characteristics.products.typical_buying_price
    products_typical_selling_price product_characteristics.products.typical_selling_price
    products_product_description product_characteristics.products.product_description
    products_other_product_details product_characteristics.products.other_product_details
    product_characteristics_product_characteristic_value product_characteristics.product_characteristic_value
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
