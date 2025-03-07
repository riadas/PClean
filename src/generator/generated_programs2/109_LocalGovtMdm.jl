using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customer_master_index_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customer_master_index_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "master customer id"], Any[0, "cmi details"], Any[1, "cmi cross reference id"], Any[1, "master customer id"], Any[1, "source system code"], Any[2, "council tax id"], Any[2, "cmi cross reference id"], Any[3, "business rates id"], Any[3, "cmi cross reference id"], Any[4, "council tax id"], Any[4, "cmi cross ref id"], Any[5, "council tax id"], Any[5, "cmi cross reference id"], Any[6, "council tax id"], Any[6, "cmi cross reference id"], Any[7, "electoral register id"], Any[7, "cmi cross reference id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "master customer id"], Any[0, "cmi details"], Any[1, "cmi cross reference id"], Any[1, "master customer id"], Any[1, "source system code"], Any[2, "council tax id"], Any[2, "cmi cross reference id"], Any[3, "business rates id"], Any[3, "cmi cross reference id"], Any[4, "council tax id"], Any[4, "cmi cross ref id"], Any[5, "council tax id"], Any[5, "cmi cross reference id"], Any[6, "council tax id"], Any[6, "cmi cross reference id"], Any[7, "electoral register id"], Any[7, "cmi cross reference id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "master customer id"], Any[0, "cmi details"], Any[1, "cmi cross reference id"], Any[1, "master customer id"], Any[1, "source system code"], Any[2, "council tax id"], Any[2, "cmi cross reference id"], Any[3, "business rates id"], Any[3, "cmi cross reference id"], Any[4, "council tax id"], Any[4, "cmi cross ref id"], Any[5, "council tax id"], Any[5, "cmi cross reference id"], Any[6, "council tax id"], Any[6, "cmi cross reference id"], Any[7, "electoral register id"], Any[7, "cmi cross reference id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "master customer id"], Any[0, "cmi details"], Any[1, "cmi cross reference id"], Any[1, "master customer id"], Any[1, "source system code"], Any[2, "council tax id"], Any[2, "cmi cross reference id"], Any[3, "business rates id"], Any[3, "cmi cross reference id"], Any[4, "council tax id"], Any[4, "cmi cross ref id"], Any[5, "council tax id"], Any[5, "cmi cross reference id"], Any[6, "council tax id"], Any[6, "cmi cross reference id"], Any[7, "electoral register id"], Any[7, "cmi cross reference id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "master customer id"], Any[0, "cmi details"], Any[1, "cmi cross reference id"], Any[1, "master customer id"], Any[1, "source system code"], Any[2, "council tax id"], Any[2, "cmi cross reference id"], Any[3, "business rates id"], Any[3, "cmi cross reference id"], Any[4, "council tax id"], Any[4, "cmi cross ref id"], Any[5, "council tax id"], Any[5, "cmi cross reference id"], Any[6, "council tax id"], Any[6, "cmi cross reference id"], Any[7, "electoral register id"], Any[7, "cmi cross reference id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[4, 1], Any[7, 3], Any[9, 3], Any[11, 3], Any[13, 3], Any[15, 3], Any[17, 3]])
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







PClean.@model LocalGovtMdmModel begin
    @class Customer_master_index begin
        cmi_details ~ ChooseUniformly(possibilities[:cmi_details])
    end

    @class Cmi_cross_references begin
        customer_master_index ~ Customer_master_index
        source_system_code ~ ChooseUniformly(possibilities[:source_system_code])
    end

    @class Council_tax begin
        cmi_cross_references ~ Cmi_cross_references
    end

    @class Business_rates begin
        cmi_cross_references ~ Cmi_cross_references
    end

    @class Benefits_overpayments begin
        cmi_cross_references ~ Cmi_cross_references
    end

    @class Parking_fines begin
        cmi_cross_references ~ Cmi_cross_references
    end

    @class Rent_arrears begin
        cmi_cross_references ~ Cmi_cross_references
    end

    @class Electoral_register begin
        cmi_cross_references ~ Cmi_cross_references
    end

    @class Obs begin
        council_tax ~ Council_tax
        business_rates ~ Business_rates
        benefits_overpayments ~ Benefits_overpayments
        parking_fines ~ Parking_fines
        rent_arrears ~ Rent_arrears
        electoral_register ~ Electoral_register
    end
end

query = @query LocalGovtMdmModel.Obs [
    customer_master_index_master_customer_id council_tax.cmi_cross_references.customer_master_index.master_customer_id
    customer_master_index_cmi_details council_tax.cmi_cross_references.customer_master_index.cmi_details
    cmi_cross_references_cmi_cross_reference_id council_tax.cmi_cross_references.cmi_cross_reference_id
    cmi_cross_references_source_system_code council_tax.cmi_cross_references.source_system_code
    council_tax_id council_tax.council_tax_id
    business_rates_id business_rates.business_rates_id
    benefits_overpayments_council_tax_id benefits_overpayments.council_tax_id
    parking_fines_council_tax_id parking_fines.council_tax_id
    rent_arrears_council_tax_id rent_arrears.council_tax_id
    electoral_register_id electoral_register.electoral_register_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
