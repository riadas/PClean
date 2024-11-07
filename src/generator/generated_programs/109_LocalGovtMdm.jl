using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customer master index_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customer master index_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "master customer id"], Any[0, "cmi details"], Any[1, "cmi cross reference id"], Any[1, "master customer id"], Any[1, "source system code"], Any[2, "council tax id"], Any[2, "cmi cross reference id"], Any[3, "business rates id"], Any[3, "cmi cross reference id"], Any[4, "council tax id"], Any[4, "cmi cross ref id"], Any[5, "council tax id"], Any[5, "cmi cross reference id"], Any[6, "council tax id"], Any[6, "cmi cross reference id"], Any[7, "electoral register id"], Any[7, "cmi cross reference id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "master customer id"], Any[0, "cmi details"], Any[1, "cmi cross reference id"], Any[1, "master customer id"], Any[1, "source system code"], Any[2, "council tax id"], Any[2, "cmi cross reference id"], Any[3, "business rates id"], Any[3, "cmi cross reference id"], Any[4, "council tax id"], Any[4, "cmi cross ref id"], Any[5, "council tax id"], Any[5, "cmi cross reference id"], Any[6, "council tax id"], Any[6, "cmi cross reference id"], Any[7, "electoral register id"], Any[7, "cmi cross reference id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Cmi_Cross_References begin
        cmi_cross_reference_id ~ Unmodeled()
        master_customer_id ~ ChooseUniformly(possibilities[:master_customer_id])
        source_system_code ~ ChooseUniformly(possibilities[:source_system_code])
    end

    @class Council_Tax begin
        council_tax_id ~ Unmodeled()
        cmi_cross_reference_id ~ ChooseUniformly(possibilities[:cmi_cross_reference_id])
    end

    @class Business_Rates begin
        business_rates_id ~ Unmodeled()
        cmi_cross_reference_id ~ ChooseUniformly(possibilities[:cmi_cross_reference_id])
    end

    @class Benefits_Overpayments begin
        council_tax_id ~ Unmodeled()
        cmi_cross_ref_id ~ ChooseUniformly(possibilities[:cmi_cross_ref_id])
    end

    @class Parking_Fines begin
        council_tax_id ~ Unmodeled()
        cmi_cross_reference_id ~ ChooseUniformly(possibilities[:cmi_cross_reference_id])
    end

    @class Rent_Arrears begin
        council_tax_id ~ Unmodeled()
        cmi_cross_reference_id ~ ChooseUniformly(possibilities[:cmi_cross_reference_id])
    end

    @class Electoral_Register begin
        electoral_register_id ~ Unmodeled()
        cmi_cross_reference_id ~ ChooseUniformly(possibilities[:cmi_cross_reference_id])
    end

    @class Obs begin
        customer_Master_Index ~ Customer_Master_Index
        cmi_Cross_References ~ Cmi_Cross_References
        council_Tax ~ Council_Tax
        business_Rates ~ Business_Rates
        benefits_Overpayments ~ Benefits_Overpayments
        parking_Fines ~ Parking_Fines
        rent_Arrears ~ Rent_Arrears
        electoral_Register ~ Electoral_Register
    end
end

query = @query LocalGovtMdmModel.Obs [
    customer_master_index_master_customer_id customer_Master_Index.master_customer_id
    customer_master_index_cmi_details customer_Master_Index.cmi_details
    cmi_cross_references_cmi_cross_reference_id cmi_Cross_References.cmi_cross_reference_id
    cmi_cross_references_source_system_code cmi_Cross_References.source_system_code
    council_tax_id council_Tax.council_tax_id
    business_rates_id business_Rates.business_rates_id
    benefits_overpayments_council_tax_id benefits_Overpayments.council_tax_id
    parking_fines_council_tax_id parking_Fines.council_tax_id
    rent_arrears_council_tax_id rent_Arrears.council_tax_id
    electoral_register_id electoral_Register.electoral_register_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
