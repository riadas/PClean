using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference characteristic types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference characteristic types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "characteristic type code"], Any[0, "characteristic type description"], Any[1, "color code"], Any[1, "color description"], Any[2, "product category code"], Any[2, "product category description"], Any[2, "unit of measure"], Any[3, "characteristic id"], Any[3, "characteristic type code"], Any[3, "characteristic data type"], Any[3, "characteristic name"], Any[3, "other characteristic details"], Any[4, "product id"], Any[4, "color code"], Any[4, "product category code"], Any[4, "product name"], Any[4, "typical buying price"], Any[4, "typical selling price"], Any[4, "product description"], Any[4, "other product details"], Any[5, "product id"], Any[5, "characteristic id"], Any[5, "product characteristic value"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "characteristic type code"], Any[0, "characteristic type description"], Any[1, "color code"], Any[1, "color description"], Any[2, "product category code"], Any[2, "product category description"], Any[2, "unit of measure"], Any[3, "characteristic id"], Any[3, "characteristic type code"], Any[3, "characteristic data type"], Any[3, "characteristic name"], Any[3, "other characteristic details"], Any[4, "product id"], Any[4, "color code"], Any[4, "product category code"], Any[4, "product name"], Any[4, "typical buying price"], Any[4, "typical selling price"], Any[4, "product description"], Any[4, "other product details"], Any[5, "product id"], Any[5, "characteristic id"], Any[5, "product characteristic value"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ProductsGenCharacteristicsModel begin
    @class Reference_Characteristic_Types begin
        characteristic_type_code ~ ChooseUniformly(possibilities[:characteristic_type_code])
        characteristic_type_description ~ ChooseUniformly(possibilities[:characteristic_type_description])
    end

    @class Reference_Colors begin
        color_code ~ ChooseUniformly(possibilities[:color_code])
        color_description ~ ChooseUniformly(possibilities[:color_description])
    end

    @class Reference_Product_Categories begin
        product_category_code ~ ChooseUniformly(possibilities[:product_category_code])
        product_category_description ~ ChooseUniformly(possibilities[:product_category_description])
        unit_of_measure ~ ChooseUniformly(possibilities[:unit_of_measure])
    end

    @class Characteristics begin
        characteristic_id ~ Unmodeled()
        characteristic_type_code ~ ChooseUniformly(possibilities[:characteristic_type_code])
        characteristic_data_type ~ ChooseUniformly(possibilities[:characteristic_data_type])
        characteristic_name ~ ChooseUniformly(possibilities[:characteristic_name])
        other_characteristic_details ~ ChooseUniformly(possibilities[:other_characteristic_details])
    end

    @class Products begin
        product_id ~ Unmodeled()
        color_code ~ ChooseUniformly(possibilities[:color_code])
        product_category_code ~ ChooseUniformly(possibilities[:product_category_code])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        typical_buying_price ~ ChooseUniformly(possibilities[:typical_buying_price])
        typical_selling_price ~ ChooseUniformly(possibilities[:typical_selling_price])
        product_description ~ ChooseUniformly(possibilities[:product_description])
        other_product_details ~ ChooseUniformly(possibilities[:other_product_details])
    end

    @class Product_Characteristics begin
        product_id ~ Unmodeled()
        characteristic_id ~ ChooseUniformly(possibilities[:characteristic_id])
        product_characteristic_value ~ ChooseUniformly(possibilities[:product_characteristic_value])
    end

    @class Obs begin
        reference_Characteristic_Types ~ Reference_Characteristic_Types
        reference_Colors ~ Reference_Colors
        reference_Product_Categories ~ Reference_Product_Categories
        characteristics ~ Characteristics
        products ~ Products
        product_Characteristics ~ Product_Characteristics
    end
end

query = @query ProductsGenCharacteristicsModel.Obs [
    reference_characteristic_types_characteristic_type_code reference_Characteristic_Types.characteristic_type_code
    reference_characteristic_types_characteristic_type_description reference_Characteristic_Types.characteristic_type_description
    reference_colors_color_code reference_Colors.color_code
    reference_colors_color_description reference_Colors.color_description
    reference_product_categories_product_category_code reference_Product_Categories.product_category_code
    reference_product_categories_product_category_description reference_Product_Categories.product_category_description
    reference_product_categories_unit_of_measure reference_Product_Categories.unit_of_measure
    characteristics_characteristic_id characteristics.characteristic_id
    characteristics_characteristic_data_type characteristics.characteristic_data_type
    characteristics_characteristic_name characteristics.characteristic_name
    characteristics_other_characteristic_details characteristics.other_characteristic_details
    products_product_id products.product_id
    products_product_name products.product_name
    products_typical_buying_price products.typical_buying_price
    products_typical_selling_price products.typical_selling_price
    products_product_description products.product_description
    products_other_product_details products.other_product_details
    product_characteristics_product_characteristic_value product_Characteristics.product_characteristic_value
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
