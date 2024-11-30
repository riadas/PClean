using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("author_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("author_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


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
foreign_keys = ["did", "aid", "did", "cid", "did", "jid", "did", "kid", "cid", "jid", "did", "pid", "kid", "pid", "aid", "pid", "citing", "cited"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "homepage"], Any[1, "name"], Any[2, "name"], Any[5, "homepage"], Any[5, "name"], Any[7, "keyword"], Any[9, "abstract"], Any[9, "citation num"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"]]
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





PClean.@model AcademicModel begin
    @class Author begin
        aid ~ Unmodeled()
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
        oid ~ ChooseUniformly(possibilities[:oid])
    end

    @class Conference begin
        cid ~ Unmodeled()
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Domain begin
        did ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Domain_Author begin
        author ~ Author
        domain ~ Domain
    end

    @class Domain_Conference begin
        conference ~ Conference
        domain ~ Domain
    end

    @class Journal begin
        homepage ~ ChooseUniformly(possibilities[:homepage])
        jid ~ ChooseUniformly(possibilities[:jid])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Domain_Journal begin
        domain ~ Domain
        journal ~ Journal
    end

    @class Keyword begin
        keyword ~ ChooseUniformly(possibilities[:keyword])
        kid ~ ChooseUniformly(possibilities[:kid])
    end

    @class Domain_Keyword begin
        domain ~ Domain
        keyword ~ Keyword
    end

    @class Publication begin
        abstract ~ ChooseUniformly(possibilities[:abstract])
        conference ~ Conference
        citation_num ~ ChooseUniformly(possibilities[:citation_num])
        journal ~ Journal
        pid ~ ChooseUniformly(possibilities[:pid])
        reference_num ~ ChooseUniformly(possibilities[:reference_num])
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Domain_Publication begin
        domain ~ Domain
        publication ~ Publication
    end

    @class Organization begin
        continent ~ ChooseUniformly(possibilities[:continent])
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
        oid ~ ChooseUniformly(possibilities[:oid])
    end

    @class Publication_Keyword begin
        publication ~ Publication
        keyword ~ Keyword
    end

    @class Writes begin
        author ~ Author
        publication ~ Publication
    end

    @class Cite begin
        publication ~ Publication
        publication ~ Publication
    end

    @class Obs begin
        domain_Author ~ Domain_Author
        domain_Conference ~ Domain_Conference
        domain_Journal ~ Domain_Journal
        domain_Keyword ~ Domain_Keyword
        domain_Publication ~ Domain_Publication
        organization ~ Organization
        publication_Keyword ~ Publication_Keyword
        writes ~ Writes
        cite ~ Cite
    end
end

query = @query AcademicModel.Obs [
    author_aid domain_Author.author.aid
    author_homepage domain_Author.author.homepage
    author_name domain_Author.author.name
    author_oid domain_Author.author.oid
    conference_cid domain_Conference.conference.cid
    conference_homepage domain_Conference.conference.homepage
    conference_name domain_Conference.conference.name
    domain_did domain_Author.domain.did
    domain_name domain_Author.domain.name
    journal_homepage domain_Journal.journal.homepage
    journal_jid domain_Journal.journal.jid
    journal_name domain_Journal.journal.name
    keyword domain_Keyword.keyword.keyword
    keyword_kid domain_Keyword.keyword.kid
    publication_abstract domain_Publication.publication.abstract
    publication_citation_num domain_Publication.publication.citation_num
    publication_pid domain_Publication.publication.pid
    publication_reference_num domain_Publication.publication.reference_num
    publication_title domain_Publication.publication.title
    publication_year domain_Publication.publication.year
    organization_continent organization.continent
    organization_homepage organization.homepage
    organization_name organization.name
    organization_oid organization.oid
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
