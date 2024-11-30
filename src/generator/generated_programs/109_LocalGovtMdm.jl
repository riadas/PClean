using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customer master index_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customer master index_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["master customer id", "cmi cross reference id", "cmi cross reference id", "cmi cross ref id", "cmi cross reference id", "cmi cross reference id", "cmi cross reference id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "cmi details"], Any[1, "source system code"], Any[2, "council tax id"], Any[3, "business rates id"], Any[4, "council tax id"], Any[5, "council tax id"], Any[6, "council tax id"], Any[7, "electoral register id"]]
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





PClean.@model LocalGovtMdmModel begin
    @class Customer_Master_Index begin
        master_customer_id ~ Unmodeled()
        cmi_details ~ ChooseUniformly(possibilities[:cmi_details])
    end

    @class Obs begin
        customer_Master_Index ~ Customer_Master_Index
        cmi_cross_reference_id ~ Unmodeled()
        source_system_code ~ ChooseUniformly(possibilities[:source_system_code])
        council_tax_id ~ Unmodeled()
        business_rates_id ~ Unmodeled()
        council_tax_id ~ Unmodeled()
        council_tax_id ~ Unmodeled()
        council_tax_id ~ Unmodeled()
        electoral_register_id ~ Unmodeled()
    end
end

query = @query LocalGovtMdmModel.Obs [
    customer_master_index_master_customer_id customer_Master_Index.master_customer_id
    customer_master_index_cmi_details customer_Master_Index.cmi_details
    cmi_cross_references_cmi_cross_reference_id cmi_cross_reference_id
    cmi_cross_references_source_system_code source_system_code
    council_tax_id council_tax_id
    business_rates_id business_rates_id
    benefits_overpayments_council_tax_id council_tax_id
    parking_fines_council_tax_id council_tax_id
    rent_arrears_council_tax_id council_tax_id
    electoral_register_id electoral_register_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
