using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("medicine_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("medicine_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "trade name"], Any[0, "fda approved"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "product"], Any[1, "chromosome"], Any[1, "omim"], Any[1, "porphyria"], Any[2, "enzyme id"], Any[2, "medicine id"], Any[2, "interaction type"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "trade name"], Any[0, "fda approved"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "product"], Any[1, "chromosome"], Any[1, "omim"], Any[1, "porphyria"], Any[2, "enzyme id"], Any[2, "medicine id"], Any[2, "interaction type"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["medicine id", "enzyme id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "trade name"], Any[0, "fda approved"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "product"], Any[1, "chromosome"], Any[1, "omim"], Any[1, "porphyria"], Any[2, "interaction type"]]
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
        enzyme ~ Enzyme
        medicine ~ Medicine
        interaction_type ~ ChooseUniformly(possibilities[:interaction_type])
    end

    @class Obs begin
        medicine_Enzyme_Interaction ~ Medicine_Enzyme_Interaction
    end
end

query = @query MedicineEnzymeInteractionModel.Obs [
    medicine_id medicine_Enzyme_Interaction.medicine.id
    medicine_name medicine_Enzyme_Interaction.medicine.name
    medicine_trade_name medicine_Enzyme_Interaction.medicine.trade_name
    medicine_fda_approved medicine_Enzyme_Interaction.medicine.fda_approved
    enzyme_id medicine_Enzyme_Interaction.enzyme.id
    enzyme_name medicine_Enzyme_Interaction.enzyme.name
    enzyme_location medicine_Enzyme_Interaction.enzyme.location
    enzyme_product medicine_Enzyme_Interaction.enzyme.product
    enzyme_chromosome medicine_Enzyme_Interaction.enzyme.chromosome
    enzyme_omim medicine_Enzyme_Interaction.enzyme.omim
    enzyme_porphyria medicine_Enzyme_Interaction.enzyme.porphyria
    medicine_enzyme_interaction_interaction_type medicine_Enzyme_Interaction.interaction_type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
