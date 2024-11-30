using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("venue_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("venue_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "venue id"], Any[0, "venue name"], Any[1, "author id"], Any[1, "author name"], Any[2, "dataset id"], Any[2, "dataset name"], Any[3, "journal id"], Any[3, "journal name"], Any[4, "key phrase id"], Any[4, "key phrase name"], Any[5, "paper id"], Any[5, "title"], Any[5, "venue id"], Any[5, "year"], Any[5, "number citing"], Any[5, "number cited by"], Any[5, "journal id"], Any[6, "citing paper id"], Any[6, "cited paper id"], Any[7, "paper id"], Any[7, "dataset id"], Any[8, "paper id"], Any[8, "key phrase id"], Any[9, "paper id"], Any[9, "author id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "venue id"], Any[0, "venue name"], Any[1, "author id"], Any[1, "author name"], Any[2, "dataset id"], Any[2, "dataset name"], Any[3, "journal id"], Any[3, "journal name"], Any[4, "key phrase id"], Any[4, "key phrase name"], Any[5, "paper id"], Any[5, "title"], Any[5, "venue id"], Any[5, "year"], Any[5, "number citing"], Any[5, "number cited by"], Any[5, "journal id"], Any[6, "citing paper id"], Any[6, "cited paper id"], Any[7, "paper id"], Any[7, "dataset id"], Any[8, "paper id"], Any[8, "key phrase id"], Any[9, "paper id"], Any[9, "author id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["venue id", "journal id", "citing paper id", "cited paper id", "key phrase id", "paper id", "author id", "paper id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "venue name"], Any[1, "author name"], Any[2, "dataset id"], Any[2, "dataset name"], Any[3, "journal name"], Any[4, "key phrase name"], Any[5, "title"], Any[5, "year"], Any[5, "number citing"], Any[5, "number cited by"], Any[7, "dataset id"]]
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





PClean.@model ScholarModel begin
    @class Venue begin
        venue_id ~ Unmodeled()
        venue_name ~ ChooseUniformly(possibilities[:venue_name])
    end

    @class Author begin
        author_id ~ Unmodeled()
        author_name ~ ChooseUniformly(possibilities[:author_name])
    end

    @class Dataset begin
        dataset_id ~ Unmodeled()
        dataset_name ~ ChooseUniformly(possibilities[:dataset_name])
    end

    @class Journal begin
        journal_id ~ Unmodeled()
        journal_name ~ ChooseUniformly(possibilities[:journal_name])
    end

    @class Key_Phrase begin
        key_phrase_id ~ Unmodeled()
        key_phrase_name ~ ChooseUniformly(possibilities[:key_phrase_name])
    end

    @class Paper_Dataset begin
        paper_id ~ Unmodeled()
        dataset_id ~ ChooseUniformly(possibilities[:dataset_id])
    end

    @class Obs begin
        venue ~ Venue
        author ~ Author
        dataset ~ Dataset
        journal ~ Journal
        key_Phrase ~ Key_Phrase
        paper_Dataset ~ Paper_Dataset
        paper_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
        number_citing ~ ChooseUniformly(possibilities[:number_citing])
        number_cited_by ~ ChooseUniformly(possibilities[:number_cited_by])
    end
end

query = @query ScholarModel.Obs [
    venue_id venue.venue_id
    venue_name venue.venue_name
    author_id author.author_id
    author_name author.author_name
    dataset_id dataset.dataset_id
    dataset_name dataset.dataset_name
    journal_id journal.journal_id
    journal_name journal.journal_name
    key_phrase_id key_Phrase.key_phrase_id
    key_phrase_name key_Phrase.key_phrase_name
    paper_id paper_id
    paper_title title
    paper_year year
    paper_number_citing number_citing
    paper_number_cited_by number_cited_by
    paper_dataset_paper_id paper_Dataset.paper_id
    paper_dataset_dataset_id paper_Dataset.dataset_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
