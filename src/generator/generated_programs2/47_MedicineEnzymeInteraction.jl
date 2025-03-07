using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("medicine_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("medicine_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "trade name"], Any[0, "fda approved"], Any[1, "id"], Any[1, "name"], Any[1, "location"], Any[1, "product"], Any[1, "chromosome"], Any[1, "omim"], Any[1, "porphyria"], Any[2, "enzyme id"], Any[2, "medicine id"], Any[2, "interaction type"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 1], Any[12, 5]])
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







PClean.@model MedicineEnzymeInteractionModel begin
    @class Medicine begin
        name ~ ChooseUniformly(possibilities[:name])
        trade_name ~ ChooseUniformly(possibilities[:trade_name])
        fda_approved ~ ChooseUniformly(possibilities[:fda_approved])
    end

    @class Enzyme begin
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        product ~ ChooseUniformly(possibilities[:product])
        chromosome ~ ChooseUniformly(possibilities[:chromosome])
        omim ~ ChooseUniformly(possibilities[:omim])
        porphyria ~ ChooseUniformly(possibilities[:porphyria])
    end

    @class Medicine_enzyme_interaction begin
        medicine ~ Medicine
        interaction_type ~ ChooseUniformly(possibilities[:interaction_type])
    end

    @class Obs begin
        medicine_enzyme_interaction ~ Medicine_enzyme_interaction
    end
end

query = @query MedicineEnzymeInteractionModel.Obs [
    medicine_name medicine_enzyme_interaction.medicine.name
    medicine_trade_name medicine_enzyme_interaction.medicine.trade_name
    medicine_fda_approved medicine_enzyme_interaction.medicine.fda_approved
    enzyme_name medicine_enzyme_interaction.enzyme.name
    enzyme_location medicine_enzyme_interaction.enzyme.location
    enzyme_product medicine_enzyme_interaction.enzyme.product
    enzyme_chromosome medicine_enzyme_interaction.enzyme.chromosome
    enzyme_omim medicine_enzyme_interaction.enzyme.omim
    enzyme_porphyria medicine_enzyme_interaction.enzyme.porphyria
    medicine_enzyme_interaction_interaction_type medicine_enzyme_interaction.interaction_type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
