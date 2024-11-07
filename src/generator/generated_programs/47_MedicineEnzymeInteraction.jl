using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("medicine_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("medicine_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "trade name"], Any[0, "fda approved"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "product"], Any[1, "chromosome"], Any[1, "omim"], Any[1, "porphyria"], Any[2, "enzyme id"], Any[2, "medicine id"], Any[2, "interaction type"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "trade name"], Any[0, "fda approved"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "product"], Any[1, "chromosome"], Any[1, "omim"], Any[1, "porphyria"], Any[2, "enzyme id"], Any[2, "medicine id"], Any[2, "interaction type"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model MedicineEnzymeInteractionModel begin
    @class Medicine begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        trade_name ~ ChooseUniformly(possibilities[:trade_name])
        fda_approved ~ ChooseUniformly(possibilities[:fda_approved])
    end

    @class Enzyme begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        product ~ ChooseUniformly(possibilities[:product])
        chromosome ~ ChooseUniformly(possibilities[:chromosome])
        omim ~ ChooseUniformly(possibilities[:omim])
        porphyria ~ ChooseUniformly(possibilities[:porphyria])
    end

    @class Medicine_Enzyme_Interaction begin
        enzyme_id ~ Unmodeled()
        medicine_id ~ ChooseUniformly(possibilities[:medicine_id])
        interaction_type ~ ChooseUniformly(possibilities[:interaction_type])
    end

    @class Obs begin
        medicine ~ Medicine
        enzyme ~ Enzyme
        medicine_Enzyme_Interaction ~ Medicine_Enzyme_Interaction
    end
end

query = @query MedicineEnzymeInteractionModel.Obs [
    medicine_id medicine.id
    medicine_name medicine.name
    medicine_trade_name medicine.trade_name
    medicine_fda_approved medicine.fda_approved
    enzyme_id enzyme.id
    enzyme_name enzyme.name
    enzyme_location enzyme.location
    enzyme_product enzyme.product
    enzyme_chromosome enzyme.chromosome
    enzyme_omim enzyme.omim
    enzyme_porphyria enzyme.porphyria
    medicine_enzyme_interaction_interaction_type medicine_Enzyme_Interaction.interaction_type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
