using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("author_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("author_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "aid"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "cid"], Any[1, "homepage"], Any[1, "name"], Any[2, "did"], Any[2, "name"], Any[3, "aid"], Any[3, "did"], Any[4, "cid"], Any[4, "did"], Any[5, "homepage"], Any[5, "jid"], Any[5, "name"], Any[6, "did"], Any[6, "jid"], Any[7, "keyword"], Any[7, "kid"], Any[8, "did"], Any[8, "kid"], Any[9, "abstract"], Any[9, "cid"], Any[9, "citation num"], Any[9, "jid"], Any[9, "pid"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[10, "did"], Any[10, "pid"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"], Any[12, "pid"], Any[12, "kid"], Any[13, "aid"], Any[13, "pid"], Any[14, "cited"], Any[14, "citing"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "aid"], Any[0, "homepage"], Any[0, "name"], Any[0, "oid"], Any[1, "cid"], Any[1, "homepage"], Any[1, "name"], Any[2, "did"], Any[2, "name"], Any[3, "aid"], Any[3, "did"], Any[4, "cid"], Any[4, "did"], Any[5, "homepage"], Any[5, "jid"], Any[5, "name"], Any[6, "did"], Any[6, "jid"], Any[7, "keyword"], Any[7, "kid"], Any[8, "did"], Any[8, "kid"], Any[9, "abstract"], Any[9, "cid"], Any[9, "citation num"], Any[9, "jid"], Any[9, "pid"], Any[9, "reference num"], Any[9, "title"], Any[9, "year"], Any[10, "did"], Any[10, "pid"], Any[11, "continent"], Any[11, "homepage"], Any[11, "name"], Any[11, "oid"], Any[12, "pid"], Any[12, "kid"], Any[13, "aid"], Any[13, "pid"], Any[14, "cited"], Any[14, "citing"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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
        aid ~ Unmodeled()
        did ~ ChooseUniformly(possibilities[:did])
    end

    @class Domain_Conference begin
        cid ~ Unmodeled()
        did ~ ChooseUniformly(possibilities[:did])
    end

    @class Journal begin
        homepage ~ ChooseUniformly(possibilities[:homepage])
        jid ~ ChooseUniformly(possibilities[:jid])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Domain_Journal begin
        did ~ Unmodeled()
        jid ~ ChooseUniformly(possibilities[:jid])
    end

    @class Keyword begin
        keyword ~ ChooseUniformly(possibilities[:keyword])
        kid ~ ChooseUniformly(possibilities[:kid])
    end

    @class Domain_Keyword begin
        did ~ Unmodeled()
        kid ~ ChooseUniformly(possibilities[:kid])
    end

    @class Publication begin
        abstract ~ ChooseUniformly(possibilities[:abstract])
        cid ~ ChooseUniformly(possibilities[:cid])
        citation_num ~ ChooseUniformly(possibilities[:citation_num])
        jid ~ ChooseUniformly(possibilities[:jid])
        pid ~ ChooseUniformly(possibilities[:pid])
        reference_num ~ ChooseUniformly(possibilities[:reference_num])
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Domain_Publication begin
        did ~ Unmodeled()
        pid ~ ChooseUniformly(possibilities[:pid])
    end

    @class Organization begin
        continent ~ ChooseUniformly(possibilities[:continent])
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
        oid ~ ChooseUniformly(possibilities[:oid])
    end

    @class Publication_Keyword begin
        pid ~ Unmodeled()
        kid ~ ChooseUniformly(possibilities[:kid])
    end

    @class Writes begin
        aid ~ Unmodeled()
        pid ~ ChooseUniformly(possibilities[:pid])
    end

    @class Cite begin
        cited ~ ChooseUniformly(possibilities[:cited])
        citing ~ ChooseUniformly(possibilities[:citing])
    end

    @class Obs begin
        author ~ Author
        conference ~ Conference
        domain ~ Domain
        domain_Author ~ Domain_Author
        domain_Conference ~ Domain_Conference
        journal ~ Journal
        domain_Journal ~ Domain_Journal
        keyword ~ Keyword
        domain_Keyword ~ Domain_Keyword
        publication ~ Publication
        domain_Publication ~ Domain_Publication
        organization ~ Organization
        publication_Keyword ~ Publication_Keyword
        writes ~ Writes
        cite ~ Cite
    end
end

query = @query AcademicModel.Obs [
    author_aid author.aid
    author_homepage author.homepage
    author_name author.name
    author_oid author.oid
    conference_cid conference.cid
    conference_homepage conference.homepage
    conference_name conference.name
    domain_did domain.did
    domain_name domain.name
    journal_homepage journal.homepage
    journal_jid journal.jid
    journal_name journal.name
    keyword keyword.keyword
    keyword_kid keyword.kid
    publication_abstract publication.abstract
    publication_citation_num publication.citation_num
    publication_pid publication.pid
    publication_reference_num publication.reference_num
    publication_title publication.title
    publication_year publication.year
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
