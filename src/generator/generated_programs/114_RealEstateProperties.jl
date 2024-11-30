using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference feature types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference feature types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "feature type code"], Any[0, "feature type name"], Any[1, "property type code"], Any[1, "property type description"], Any[2, "feature id"], Any[2, "feature type code"], Any[2, "feature name"], Any[2, "feature description"], Any[3, "property id"], Any[3, "property type code"], Any[3, "date on market"], Any[3, "date sold"], Any[3, "property name"], Any[3, "property address"], Any[3, "room count"], Any[3, "vendor requested price"], Any[3, "buyer offered price"], Any[3, "agreed selling price"], Any[3, "apt feature 1"], Any[3, "apt feature 2"], Any[3, "apt feature 3"], Any[3, "fld feature 1"], Any[3, "fld feature 2"], Any[3, "fld feature 3"], Any[3, "hse feature 1"], Any[3, "hse feature 2"], Any[3, "hse feature 3"], Any[3, "oth feature 1"], Any[3, "oth feature 2"], Any[3, "oth feature 3"], Any[3, "shp feature 1"], Any[3, "shp feature 2"], Any[3, "shp feature 3"], Any[3, "other property details"], Any[4, "property id"], Any[4, "feature id"], Any[4, "property feature description"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "feature type code"], Any[0, "feature type name"], Any[1, "property type code"], Any[1, "property type description"], Any[2, "feature id"], Any[2, "feature type code"], Any[2, "feature name"], Any[2, "feature description"], Any[3, "property id"], Any[3, "property type code"], Any[3, "date on market"], Any[3, "date sold"], Any[3, "property name"], Any[3, "property address"], Any[3, "room count"], Any[3, "vendor requested price"], Any[3, "buyer offered price"], Any[3, "agreed selling price"], Any[3, "apt feature 1"], Any[3, "apt feature 2"], Any[3, "apt feature 3"], Any[3, "fld feature 1"], Any[3, "fld feature 2"], Any[3, "fld feature 3"], Any[3, "hse feature 1"], Any[3, "hse feature 2"], Any[3, "hse feature 3"], Any[3, "oth feature 1"], Any[3, "oth feature 2"], Any[3, "oth feature 3"], Any[3, "shp feature 1"], Any[3, "shp feature 2"], Any[3, "shp feature 3"], Any[3, "other property details"], Any[4, "property id"], Any[4, "feature id"], Any[4, "property feature description"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["feature type code", "property type code", "property id", "feature id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "feature type name"], Any[1, "property type description"], Any[2, "feature name"], Any[2, "feature description"], Any[3, "date on market"], Any[3, "date sold"], Any[3, "property name"], Any[3, "property address"], Any[3, "room count"], Any[3, "vendor requested price"], Any[3, "buyer offered price"], Any[3, "agreed selling price"], Any[3, "apt feature 1"], Any[3, "apt feature 2"], Any[3, "apt feature 3"], Any[3, "fld feature 1"], Any[3, "fld feature 2"], Any[3, "fld feature 3"], Any[3, "hse feature 1"], Any[3, "hse feature 2"], Any[3, "hse feature 3"], Any[3, "oth feature 1"], Any[3, "oth feature 2"], Any[3, "oth feature 3"], Any[3, "shp feature 1"], Any[3, "shp feature 2"], Any[3, "shp feature 3"], Any[3, "other property details"], Any[4, "property feature description"]]
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





PClean.@model RealEstatePropertiesModel begin
    @class Reference_Feature_Types begin
        feature_type_code ~ ChooseUniformly(possibilities[:feature_type_code])
        feature_type_name ~ ChooseUniformly(possibilities[:feature_type_name])
    end

    @class Reference_Property_Types begin
        property_type_code ~ ChooseUniformly(possibilities[:property_type_code])
        property_type_description ~ ChooseUniformly(possibilities[:property_type_description])
    end

    @class Obs begin
        reference_Feature_Types ~ Reference_Feature_Types
        reference_Property_Types ~ Reference_Property_Types
        feature_id ~ Unmodeled()
        feature_name ~ ChooseUniformly(possibilities[:feature_name])
        feature_description ~ ChooseUniformly(possibilities[:feature_description])
        property_id ~ Unmodeled()
        date_on_market ~ TimePrior(possibilities[:date_on_market])
        date_sold ~ TimePrior(possibilities[:date_sold])
        property_name ~ ChooseUniformly(possibilities[:property_name])
        property_address ~ ChooseUniformly(possibilities[:property_address])
        room_count ~ ChooseUniformly(possibilities[:room_count])
        vendor_requested_price ~ ChooseUniformly(possibilities[:vendor_requested_price])
        buyer_offered_price ~ ChooseUniformly(possibilities[:buyer_offered_price])
        agreed_selling_price ~ ChooseUniformly(possibilities[:agreed_selling_price])
        apt_feature_1 ~ ChooseUniformly(possibilities[:apt_feature_1])
        apt_feature_2 ~ ChooseUniformly(possibilities[:apt_feature_2])
        apt_feature_3 ~ ChooseUniformly(possibilities[:apt_feature_3])
        fld_feature_1 ~ ChooseUniformly(possibilities[:fld_feature_1])
        fld_feature_2 ~ ChooseUniformly(possibilities[:fld_feature_2])
        fld_feature_3 ~ ChooseUniformly(possibilities[:fld_feature_3])
        hse_feature_1 ~ ChooseUniformly(possibilities[:hse_feature_1])
        hse_feature_2 ~ ChooseUniformly(possibilities[:hse_feature_2])
        hse_feature_3 ~ ChooseUniformly(possibilities[:hse_feature_3])
        oth_feature_1 ~ ChooseUniformly(possibilities[:oth_feature_1])
        oth_feature_2 ~ ChooseUniformly(possibilities[:oth_feature_2])
        oth_feature_3 ~ ChooseUniformly(possibilities[:oth_feature_3])
        shp_feature_1 ~ ChooseUniformly(possibilities[:shp_feature_1])
        shp_feature_2 ~ ChooseUniformly(possibilities[:shp_feature_2])
        shp_feature_3 ~ ChooseUniformly(possibilities[:shp_feature_3])
        other_property_details ~ ChooseUniformly(possibilities[:other_property_details])
        property_feature_description ~ ChooseUniformly(possibilities[:property_feature_description])
    end
end

query = @query RealEstatePropertiesModel.Obs [
    reference_feature_types_feature_type_code reference_Feature_Types.feature_type_code
    reference_feature_types_feature_type_name reference_Feature_Types.feature_type_name
    reference_property_types_property_type_code reference_Property_Types.property_type_code
    reference_property_types_property_type_description reference_Property_Types.property_type_description
    other_available_features_feature_id feature_id
    other_available_features_feature_name feature_name
    other_available_features_feature_description feature_description
    properties_property_id property_id
    properties_date_on_market date_on_market
    properties_date_sold date_sold
    properties_property_name property_name
    properties_property_address property_address
    properties_room_count room_count
    properties_vendor_requested_price vendor_requested_price
    properties_buyer_offered_price buyer_offered_price
    properties_agreed_selling_price agreed_selling_price
    properties_apt_feature_1 apt_feature_1
    properties_apt_feature_2 apt_feature_2
    properties_apt_feature_3 apt_feature_3
    properties_fld_feature_1 fld_feature_1
    properties_fld_feature_2 fld_feature_2
    properties_fld_feature_3 fld_feature_3
    properties_hse_feature_1 hse_feature_1
    properties_hse_feature_2 hse_feature_2
    properties_hse_feature_3 hse_feature_3
    properties_oth_feature_1 oth_feature_1
    properties_oth_feature_2 oth_feature_2
    properties_oth_feature_3 oth_feature_3
    properties_shp_feature_1 shp_feature_1
    properties_shp_feature_2 shp_feature_2
    properties_shp_feature_3 shp_feature_3
    properties_other_property_details other_property_details
    other_property_features_property_feature_description property_feature_description
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
