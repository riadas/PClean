using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("attribute_definitions_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("attribute_definitions_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "attribute id"], Any[0, "attribute name"], Any[0, "attribute data type"], Any[1, "catalog id"], Any[1, "catalog name"], Any[1, "catalog publisher"], Any[1, "date of publication"], Any[1, "date of latest revision"], Any[2, "catalog level number"], Any[2, "catalog id"], Any[2, "catalog level name"], Any[3, "catalog entry id"], Any[3, "catalog level number"], Any[3, "parent entry id"], Any[3, "previous entry id"], Any[3, "next entry id"], Any[3, "catalog entry name"], Any[3, "product stock number"], Any[3, "price in dollars"], Any[3, "price in euros"], Any[3, "price in pounds"], Any[3, "capacity"], Any[3, "length"], Any[3, "height"], Any[3, "width"], Any[4, "catalog entry id"], Any[4, "catalog level number"], Any[4, "attribute id"], Any[4, "attribute value"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[10, 4], Any[13, 9], Any[27, 9], Any[26, 12]])
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







PClean.@model ProductCatalogModel begin
    @class Attribute_definitions begin
        attribute_name ~ ChooseUniformly(possibilities[:attribute_name])
        attribute_data_type ~ ChooseUniformly(possibilities[:attribute_data_type])
    end

    @class Catalogs begin
        catalog_name ~ ChooseUniformly(possibilities[:catalog_name])
        catalog_publisher ~ ChooseUniformly(possibilities[:catalog_publisher])
        date_of_publication ~ TimePrior(possibilities[:date_of_publication])
        date_of_latest_revision ~ TimePrior(possibilities[:date_of_latest_revision])
    end

    @class Catalog_structure begin
        catalog_level_number ~ ChooseUniformly(possibilities[:catalog_level_number])
        catalogs ~ Catalogs
        catalog_level_name ~ ChooseUniformly(possibilities[:catalog_level_name])
    end

    @class Catalog_contents begin
        catalog_structure ~ Catalog_structure
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
    end

    @class Catalog_contents_additional_attributes begin
        catalog_contents ~ Catalog_contents
        catalog_structure ~ Catalog_structure
        attribute_id ~ ChooseUniformly(possibilities[:attribute_id])
        attribute_value ~ ChooseUniformly(possibilities[:attribute_value])
    end

    @class Obs begin
        attribute_definitions ~ Attribute_definitions
        catalog_contents_additional_attributes ~ Catalog_contents_additional_attributes
    end
end

query = @query ProductCatalogModel.Obs [
    attribute_definitions_attribute_id attribute_definitions.attribute_id
    attribute_definitions_attribute_name attribute_definitions.attribute_name
    attribute_definitions_attribute_data_type attribute_definitions.attribute_data_type
    catalogs_catalog_id catalog_contents_additional_attributes.catalog_contents.catalog_structure.catalogs.catalog_id
    catalogs_catalog_name catalog_contents_additional_attributes.catalog_contents.catalog_structure.catalogs.catalog_name
    catalogs_catalog_publisher catalog_contents_additional_attributes.catalog_contents.catalog_structure.catalogs.catalog_publisher
    catalogs_date_of_publication catalog_contents_additional_attributes.catalog_contents.catalog_structure.catalogs.date_of_publication
    catalogs_date_of_latest_revision catalog_contents_additional_attributes.catalog_contents.catalog_structure.catalogs.date_of_latest_revision
    catalog_structure_catalog_level_number catalog_contents_additional_attributes.catalog_contents.catalog_structure.catalog_level_number
    catalog_structure_catalog_level_name catalog_contents_additional_attributes.catalog_contents.catalog_structure.catalog_level_name
    catalog_contents_catalog_entry_id catalog_contents_additional_attributes.catalog_contents.catalog_entry_id
    catalog_contents_parent_entry_id catalog_contents_additional_attributes.catalog_contents.parent_entry_id
    catalog_contents_previous_entry_id catalog_contents_additional_attributes.catalog_contents.previous_entry_id
    catalog_contents_next_entry_id catalog_contents_additional_attributes.catalog_contents.next_entry_id
    catalog_contents_catalog_entry_name catalog_contents_additional_attributes.catalog_contents.catalog_entry_name
    catalog_contents_product_stock_number catalog_contents_additional_attributes.catalog_contents.product_stock_number
    catalog_contents_price_in_dollars catalog_contents_additional_attributes.catalog_contents.price_in_dollars
    catalog_contents_price_in_euros catalog_contents_additional_attributes.catalog_contents.price_in_euros
    catalog_contents_price_in_pounds catalog_contents_additional_attributes.catalog_contents.price_in_pounds
    catalog_contents_capacity catalog_contents_additional_attributes.catalog_contents.capacity
    catalog_contents_length catalog_contents_additional_attributes.catalog_contents.length
    catalog_contents_height catalog_contents_additional_attributes.catalog_contents.height
    catalog_contents_width catalog_contents_additional_attributes.catalog_contents.width
    catalog_contents_additional_attributes_attribute_id catalog_contents_additional_attributes.attribute_id
    catalog_contents_additional_attributes_attribute_value catalog_contents_additional_attributes.attribute_value
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
