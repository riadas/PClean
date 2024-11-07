using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference template types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference template types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "template type code"], Any[0, "template type description"], Any[1, "template id"], Any[1, "version number"], Any[1, "template type code"], Any[1, "date effective from"], Any[1, "date effective to"], Any[1, "template details"], Any[2, "document id"], Any[2, "template id"], Any[2, "document name"], Any[2, "document description"], Any[2, "other details"], Any[3, "paragraph id"], Any[3, "document id"], Any[3, "paragraph text"], Any[3, "other details"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "template type code"], Any[0, "template type description"], Any[1, "template id"], Any[1, "version number"], Any[1, "template type code"], Any[1, "date effective from"], Any[1, "date effective to"], Any[1, "template details"], Any[2, "document id"], Any[2, "template id"], Any[2, "document name"], Any[2, "document description"], Any[2, "other details"], Any[3, "paragraph id"], Any[3, "document id"], Any[3, "paragraph text"], Any[3, "other details"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Templates begin
        template_id ~ Unmodeled()
        version_number ~ ChooseUniformly(possibilities[:version_number])
        template_type_code ~ ChooseUniformly(possibilities[:template_type_code])
        date_effective_from ~ TimePrior(possibilities[:date_effective_from])
        date_effective_to ~ TimePrior(possibilities[:date_effective_to])
        template_details ~ ChooseUniformly(possibilities[:template_details])
    end

    @class Documents begin
        document_id ~ Unmodeled()
        template_id ~ ChooseUniformly(possibilities[:template_id])
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Paragraphs begin
        paragraph_id ~ Unmodeled()
        document_id ~ ChooseUniformly(possibilities[:document_id])
        paragraph_text ~ ChooseUniformly(possibilities[:paragraph_text])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        reference_Template_Types ~ Reference_Template_Types
        templates ~ Templates
        documents ~ Documents
        paragraphs ~ Paragraphs
    end
end

query = @query CreDocTemplateMgtModel.Obs [
    reference_template_types_template_type_code reference_Template_Types.template_type_code
    reference_template_types_template_type_description reference_Template_Types.template_type_description
    templates_template_id templates.template_id
    templates_version_number templates.version_number
    templates_date_effective_from templates.date_effective_from
    templates_date_effective_to templates.date_effective_to
    templates_template_details templates.template_details
    documents_document_id documents.document_id
    documents_document_name documents.document_name
    documents_document_description documents.document_description
    documents_other_details documents.other_details
    paragraphs_paragraph_id paragraphs.paragraph_id
    paragraphs_paragraph_text paragraphs.paragraph_text
    paragraphs_other_details paragraphs.other_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
