using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("reference_template_types_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("reference_template_types_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "template type code"], Any[0, "template type description"], Any[1, "template id"], Any[1, "version number"], Any[1, "template type code"], Any[1, "date effective from"], Any[1, "date effective to"], Any[1, "template details"], Any[2, "document id"], Any[2, "template id"], Any[2, "document name"], Any[2, "document description"], Any[2, "other details"], Any[3, "paragraph id"], Any[3, "document id"], Any[3, "paragraph text"], Any[3, "other details"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 1], Any[10, 3], Any[15, 9]])
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







PClean.@model CreDocTemplateMgtModel begin
    @class Reference_template_types begin
        template_type_code ~ ChooseUniformly(possibilities[:template_type_code])
        template_type_description ~ ChooseUniformly(possibilities[:template_type_description])
    end

    @class Templates begin
        version_number ~ ChooseUniformly(possibilities[:version_number])
        reference_template_types ~ Reference_template_types
        date_effective_from ~ TimePrior(possibilities[:date_effective_from])
        date_effective_to ~ TimePrior(possibilities[:date_effective_to])
        template_details ~ ChooseUniformly(possibilities[:template_details])
    end

    @class Documents begin
        templates ~ Templates
        document_name ~ ChooseUniformly(possibilities[:document_name])
        document_description ~ ChooseUniformly(possibilities[:document_description])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Paragraphs begin
        documents ~ Documents
        paragraph_text ~ ChooseUniformly(possibilities[:paragraph_text])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Obs begin
        paragraphs ~ Paragraphs
    end
end

query = @query CreDocTemplateMgtModel.Obs [
    reference_template_types_template_type_code paragraphs.documents.templates.reference_template_types.template_type_code
    reference_template_types_template_type_description paragraphs.documents.templates.reference_template_types.template_type_description
    templates_template_id paragraphs.documents.templates.template_id
    templates_version_number paragraphs.documents.templates.version_number
    templates_date_effective_from paragraphs.documents.templates.date_effective_from
    templates_date_effective_to paragraphs.documents.templates.date_effective_to
    templates_template_details paragraphs.documents.templates.template_details
    documents_document_id paragraphs.documents.document_id
    documents_document_name paragraphs.documents.document_name
    documents_document_description paragraphs.documents.document_description
    documents_other_details paragraphs.documents.other_details
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
