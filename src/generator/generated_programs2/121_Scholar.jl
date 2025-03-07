using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("venue_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("venue_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "venue id"], Any[0, "venue name"], Any[1, "author id"], Any[1, "author name"], Any[2, "dataset id"], Any[2, "dataset name"], Any[3, "journal id"], Any[3, "journal name"], Any[4, "key phrase id"], Any[4, "key phrase name"], Any[5, "paper id"], Any[5, "title"], Any[5, "venue id"], Any[5, "year"], Any[5, "number citing"], Any[5, "number cited by"], Any[5, "journal id"], Any[6, "citing paper id"], Any[6, "cited paper id"], Any[7, "paper id"], Any[7, "dataset id"], Any[8, "paper id"], Any[8, "key phrase id"], Any[9, "paper id"], Any[9, "author id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 1], Any[17, 7], Any[18, 11], Any[19, 11], Any[23, 9], Any[22, 11], Any[25, 3], Any[24, 11]])
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







PClean.@model ScholarModel begin
    @class Venue begin
        venue_name ~ ChooseUniformly(possibilities[:venue_name])
    end

    @class Author begin
        author_name ~ ChooseUniformly(possibilities[:author_name])
    end

    @class Dataset begin
        dataset_name ~ ChooseUniformly(possibilities[:dataset_name])
    end

    @class Journal begin
        journal_name ~ ChooseUniformly(possibilities[:journal_name])
    end

    @class Key_phrase begin
        key_phrase_name ~ ChooseUniformly(possibilities[:key_phrase_name])
    end

    @class Paper begin
        title ~ ChooseUniformly(possibilities[:title])
        venue ~ Venue
        year ~ ChooseUniformly(possibilities[:year])
        number_citing ~ ChooseUniformly(possibilities[:number_citing])
        number_cited_by ~ ChooseUniformly(possibilities[:number_cited_by])
        journal ~ Journal
    end

    @class Cite begin
        paper ~ Paper
    end

    @class Paper_dataset begin
        paper_id ~ Unmodeled()
    end

    @class Paper_key_phrase begin
        paper ~ Paper
    end

    @class Writes begin
        author ~ Author
    end

    @class Obs begin
        dataset ~ Dataset
        cite ~ Cite
        paper_dataset ~ Paper_dataset
        paper_key_phrase ~ Paper_key_phrase
        writes ~ Writes
    end
end

query = @query ScholarModel.Obs [
    venue_id cite.paper.venue.venue_id
    venue_name cite.paper.venue.venue_name
    author_id writes.author.author_id
    author_name writes.author.author_name
    dataset_id dataset.dataset_id
    dataset_name dataset.dataset_name
    journal_id cite.paper.journal.journal_id
    journal_name cite.paper.journal.journal_name
    key_phrase_id paper_key_phrase.key_phrase.key_phrase_id
    key_phrase_name paper_key_phrase.key_phrase.key_phrase_name
    paper_id cite.paper.paper_id
    paper_title cite.paper.title
    paper_year cite.paper.year
    paper_number_citing cite.paper.number_citing
    paper_number_cited_by cite.paper.number_cited_by
    paper_dataset_paper_id paper_dataset.paper_id
    paper_dataset_dataset_id paper_dataset.dataset_id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
