using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("attribute definitions_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("attribute definitions_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "attribute id"], Any[0, "attribute name"], Any[0, "attribute data type"], Any[1, "catalog id"], Any[1, "catalog name"], Any[1, "catalog publisher"], Any[1, "date of publication"], Any[1, "date of latest revision"], Any[2, "catalog level number"], Any[2, "catalog id"], Any[2, "catalog level name"], Any[3, "catalog entry id"], Any[3, "catalog level number"], Any[3, "parent entry id"], Any[3, "previous entry id"], Any[3, "next entry id"], Any[3, "catalog entry name"], Any[3, "product stock number"], Any[3, "price in dollars"], Any[3, "price in euros"], Any[3, "price in pounds"], Any[3, "capacity"], Any[3, "length"], Any[3, "height"], Any[3, "width"], Any[4, "catalog entry id"], Any[4, "catalog level number"], Any[4, "attribute id"], Any[4, "attribute value"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "attribute id"], Any[0, "attribute name"], Any[0, "attribute data type"], Any[1, "catalog id"], Any[1, "catalog name"], Any[1, "catalog publisher"], Any[1, "date of publication"], Any[1, "date of latest revision"], Any[2, "catalog level number"], Any[2, "catalog id"], Any[2, "catalog level name"], Any[3, "catalog entry id"], Any[3, "catalog level number"], Any[3, "parent entry id"], Any[3, "previous entry id"], Any[3, "next entry id"], Any[3, "catalog entry name"], Any[3, "product stock number"], Any[3, "price in dollars"], Any[3, "price in euros"], Any[3, "price in pounds"], Any[3, "capacity"], Any[3, "length"], Any[3, "height"], Any[3, "width"], Any[4, "catalog entry id"], Any[4, "catalog level number"], Any[4, "attribute id"], Any[4, "attribute value"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["catalog id", "catalog level number", "catalog level number", "catalog entry id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "attribute id"], Any[0, "attribute name"], Any[0, "attribute data type"], Any[1, "catalog name"], Any[1, "catalog publisher"], Any[1, "date of publication"], Any[1, "date of latest revision"], Any[2, "catalog level name"], Any[3, "parent entry id"], Any[3, "previous entry id"], Any[3, "next entry id"], Any[3, "catalog entry name"], Any[3, "product stock number"], Any[3, "price in dollars"], Any[3, "price in euros"], Any[3, "price in pounds"], Any[3, "capacity"], Any[3, "length"], Any[3, "height"], Any[3, "width"], Any[4, "attribute id"], Any[4, "attribute value"]]
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





PClean.@model ProductCatalogModel begin
    @class Attribute_Definitions begin
        attribute_id ~ Unmodeled()
        attribute_name ~ ChooseUniformly(possibilities[:attribute_name])
        attribute_data_type ~ ChooseUniformly(possibilities[:attribute_data_type])
    end

    @class Catalogs begin
        catalog_id ~ Unmodeled()
        catalog_name ~ ChooseUniformly(possibilities[:catalog_name])
        catalog_publisher ~ ChooseUniformly(possibilities[:catalog_publisher])
        date_of_publication ~ TimePrior(possibilities[:date_of_publication])
        date_of_latest_revision ~ TimePrior(possibilities[:date_of_latest_revision])
    end

    @class Obs begin
        attribute_Definitions ~ Attribute_Definitions
        catalogs ~ Catalogs
        catalog_level_number ~ ChooseUniformly(possibilities[:catalog_level_number])
        catalog_level_name ~ ChooseUniformly(possibilities[:catalog_level_name])
        catalog_entry_id ~ Unmodeled()
        parent_entry_id ~ ChooseUniformly(possibilities[:parent_entry_id])
        previous_entry_id ~ ChooseUniformly(possibilities[:previous_entry_id])
        next_entry_id ~ ChooseUniformly(possibilities[:next_entry_id])
        catalog_entry_name ~ ChooseUniformly(possibilities[:catalog_entry_name])
        product_stock_number ~ ChooseUniformly(possibilities[:product_stock_number])
        price_in_dollars ~ ChooseUniformly(possibilities[:price_in_dollars])
        price_in_euros ~ ChooseUniformly(possibilities[:price_in_euros])
        price_in_pounds ~ ChooseUniformly(possibilities[:price_in_pounds])
        capacity ~ ChooseUniformly(possibilities[:capacity])
        length ~ ChooseUniformly(possibilities[:length])
        height ~ ChooseUniformly(possibilities[:height])
        width ~ ChooseUniformly(possibilities[:width])
        attribute_id ~ ChooseUniformly(possibilities[:attribute_id])
        attribute_value ~ ChooseUniformly(possibilities[:attribute_value])
    end
end

query = @query ProductCatalogModel.Obs [
    attribute_definitions_attribute_id attribute_Definitions.attribute_id
    attribute_definitions_attribute_name attribute_Definitions.attribute_name
    attribute_definitions_attribute_data_type attribute_Definitions.attribute_data_type
    catalogs_catalog_id catalogs.catalog_id
    catalogs_catalog_name catalogs.catalog_name
    catalogs_catalog_publisher catalogs.catalog_publisher
    catalogs_date_of_publication catalogs.date_of_publication
    catalogs_date_of_latest_revision catalogs.date_of_latest_revision
    catalog_structure_catalog_level_number catalog_level_number
    catalog_structure_catalog_level_name catalog_level_name
    catalog_contents_catalog_entry_id catalog_entry_id
    catalog_contents_parent_entry_id parent_entry_id
    catalog_contents_previous_entry_id previous_entry_id
    catalog_contents_next_entry_id next_entry_id
    catalog_contents_catalog_entry_name catalog_entry_name
    catalog_contents_product_stock_number product_stock_number
    catalog_contents_price_in_dollars price_in_dollars
    catalog_contents_price_in_euros price_in_euros
    catalog_contents_price_in_pounds price_in_pounds
    catalog_contents_capacity capacity
    catalog_contents_length length
    catalog_contents_height height
    catalog_contents_width width
    catalog_contents_additional_attributes_attribute_id attribute_id
    catalog_contents_additional_attributes_attribute_value attribute_value
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
