using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference template types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference template types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "template type code"], Any[0, "template type description"], Any[1, "template id"], Any[1, "version number"], Any[1, "template type code"], Any[1, "date effective from"], Any[1, "date effective to"], Any[1, "template details"], Any[2, "document id"], Any[2, "template id"], Any[2, "document name"], Any[2, "document description"], Any[2, "other details"], Any[3, "paragraph id"], Any[3, "document id"], Any[3, "paragraph text"], Any[3, "other details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "template type code"], Any[0, "template type description"], Any[1, "template id"], Any[1, "version number"], Any[1, "template type code"], Any[1, "date effective from"], Any[1, "date effective to"], Any[1, "template details"], Any[2, "document id"], Any[2, "template id"], Any[2, "document name"], Any[2, "document description"], Any[2, "other details"], Any[3, "paragraph id"], Any[3, "document id"], Any[3, "paragraph text"], Any[3, "other details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["template type code", "template id", "document id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "template type description"], Any[1, "version number"], Any[1, "date effective from"], Any[1, "date effective to"], Any[1, "template details"], Any[2, "document name"], Any[2, "document description"], Any[2, "other details"], Any[3, "paragraph id"], Any[3, "paragraph text"], Any[3, "other details"]]
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





PClean.@model CreDocTemplateMgtModel begin
    @class Reference_Template_Types begin
        template_type_code ~ ChooseUniformly(possibilities[:template_type_code])
        template_type_description ~ ChooseUniformly(possibilities[:template_type_description])
    end

    @class Obs begin
        reference_Template_Types ~ Reference_Template_Types
        template_id ~ Unmodeled()
        version_number ~ ChooseUniformly(possibilities[:version_number])
        date_effective_from ~ TimePrior(possibilities[:date_effective_from])
        date_effective_to ~ TimePrior(possibilities[:date_effective_to])
        template_details ~ ChooseUniformly(possibilities[:template_details])
        document_id ~ Unmodeled()
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
        paragraph_id ~ Unmodeled()
        paragraph_text ~ ChooseUniformly(possibilities[:paragraph_text])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end
end

query = @query CreDocTemplateMgtModel.Obs [
    reference_template_types_template_type_code reference_Template_Types.template_type_code
    reference_template_types_template_type_description reference_Template_Types.template_type_description
    templates_template_id template_id
    templates_version_number version_number
    templates_date_effective_from date_effective_from
    templates_date_effective_to date_effective_to
    templates_template_details template_details
    documents_document_id document_id
    documents_document_name document_name
    documents_document_description document_description
    documents_other_details other_details
    paragraphs_paragraph_id paragraph_id
    paragraphs_paragraph_text paragraph_text
    paragraphs_other_details other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
