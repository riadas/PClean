using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("author_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("author_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "aid"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "cid"], Any[1, "homepage"], Any[1, "name"], Any[2, "did"], Any[2, "name"], Any[3, "aid"], Any[3, "did"], Any[4, "cid"], Any[4, "did"], Any[5, "homepage"], Any[5, "jid"], Any[5, "name"], Any[6, "did"], Any[6, "jid"], Any[7, "keyword"], Any[7, "kid"], Any[8, "did"], Any[8, "kid"], Any[9, "abstract"], Any[9, "cid"], Any[9, "citation num"], Any[9, "jid"], Any[9, "pid"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[10, "did"], Any[10, "pid"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"], Any[12, "pid"], Any[12, "kid"], Any[13, "aid"], Any[13, "pid"], Any[14, "cited"], Any[14, "citing"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "aid"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "cid"], Any[1, "homepage"], Any[1, "name"], Any[2, "did"], Any[2, "name"], Any[3, "aid"], Any[3, "did"], Any[4, "cid"], Any[4, "did"], Any[5, "homepage"], Any[5, "jid"], Any[5, "name"], Any[6, "did"], Any[6, "jid"], Any[7, "keyword"], Any[7, "kid"], Any[8, "did"], Any[8, "kid"], Any[9, "abstract"], Any[9, "cid"], Any[9, "citation num"], Any[9, "jid"], Any[9, "pid"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[10, "did"], Any[10, "pid"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"], Any[12, "pid"], Any[12, "kid"], Any[13, "aid"], Any[13, "pid"], Any[14, "cited"], Any[14, "citing"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "aid"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "cid"], Any[1, "homepage"], Any[1, "name"], Any[2, "did"], Any[2, "name"], Any[3, "aid"], Any[3, "did"], Any[4, "cid"], Any[4, "did"], Any[5, "homepage"], Any[5, "jid"], Any[5, "name"], Any[6, "did"], Any[6, "jid"], Any[7, "keyword"], Any[7, "kid"], Any[8, "did"], Any[8, "kid"], Any[9, "abstract"], Any[9, "cid"], Any[9, "citation num"], Any[9, "jid"], Any[9, "pid"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[10, "did"], Any[10, "pid"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"], Any[12, "pid"], Any[12, "kid"], Any[13, "aid"], Any[13, "pid"], Any[14, "cited"], Any[14, "citing"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "aid"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "cid"], Any[1, "homepage"], Any[1, "name"], Any[2, "did"], Any[2, "name"], Any[3, "aid"], Any[3, "did"], Any[4, "cid"], Any[4, "did"], Any[5, "homepage"], Any[5, "jid"], Any[5, "name"], Any[6, "did"], Any[6, "jid"], Any[7, "keyword"], Any[7, "kid"], Any[8, "did"], Any[8, "kid"], Any[9, "abstract"], Any[9, "cid"], Any[9, "citation num"], Any[9, "jid"], Any[9, "pid"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[10, "did"], Any[10, "pid"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"], Any[12, "pid"], Any[12, "kid"], Any[13, "aid"], Any[13, "pid"], Any[14, "cited"], Any[14, "citing"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "aid"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "cid"], Any[1, "homepage"], Any[1, "name"], Any[2, "did"], Any[2, "name"], Any[3, "aid"], Any[3, "did"], Any[4, "cid"], Any[4, "did"], Any[5, "homepage"], Any[5, "jid"], Any[5, "name"], Any[6, "did"], Any[6, "jid"], Any[7, "keyword"], Any[7, "kid"], Any[8, "did"], Any[8, "kid"], Any[9, "abstract"], Any[9, "cid"], Any[9, "citation num"], Any[9, "jid"], Any[9, "pid"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[10, "did"], Any[10, "pid"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"], Any[12, "pid"], Any[12, "kid"], Any[13, "aid"], Any[13, "pid"], Any[14, "cited"], Any[14, "citing"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 8], Any[10, 1], Any[13, 8], Any[12, 5], Any[17, 8], Any[18, 15], Any[21, 8], Any[22, 20], Any[24, 5], Any[26, 15], Any[31, 8], Any[32, 27], Any[38, 20], Any[37, 27], Any[39, 1], Any[40, 27], Any[42, 27], Any[41, 27]])
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







PClean.@model AcademicModel begin
    @class Author begin
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
        oid ~ ChooseUniformly(possibilities[:oid])
    end

    @class Conference begin
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Domain begin
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Domain_author begin
        author ~ Author
    end

    @class Domain_conference begin
        conference ~ Conference
    end

    @class Journal begin
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Domain_journal begin
        journal ~ Journal
    end

    @class Keyword begin
        keyword ~ ChooseUniformly(possibilities[:keyword])
    end

    @class Domain_keyword begin
        keyword ~ Keyword
    end

    @class Publication begin
        abstract ~ ChooseUniformly(possibilities[:abstract])
        conference ~ Conference
        citation_num ~ ChooseUniformly(possibilities[:citation_num])
        journal ~ Journal
        reference_num ~ ChooseUniformly(possibilities[:reference_num])
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Domain_publication begin
        publication ~ Publication
    end

    @class Organization begin
        continent ~ ChooseUniformly(possibilities[:continent])
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Publication_keyword begin
        publication ~ Publication
    end

    @class Writes begin
        publication ~ Publication
    end

    @class Cite begin
        publication ~ Publication
    end

    @class Obs begin
        domain_author ~ Domain_author
        domain_conference ~ Domain_conference
        domain_journal ~ Domain_journal
        domain_keyword ~ Domain_keyword
        domain_publication ~ Domain_publication
        organization ~ Organization
        publication_keyword ~ Publication_keyword
        writes ~ Writes
        cite ~ Cite
    end
end

query = @query AcademicModel.Obs [
    author_homepage domain_author.author.homepage
    author_name domain_author.author.name
    author_oid domain_author.author.oid
    conference_homepage domain_conference.conference.homepage
    conference_name domain_conference.conference.name
    domain_name domain_author.domain.name
    journal_homepage domain_journal.journal.homepage
    journal_name domain_journal.journal.name
    keyword domain_keyword.keyword.keyword
    publication_abstract domain_publication.publication.abstract
    publication_citation_num domain_publication.publication.citation_num
    publication_reference_num domain_publication.publication.reference_num
    publication_title domain_publication.publication.title
    publication_year domain_publication.publication.year
    organization_continent organization.continent
    organization_homepage organization.homepage
    organization_name organization.name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
