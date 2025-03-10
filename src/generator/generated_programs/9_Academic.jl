using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("author_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("author_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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

    @class Journal begin
        homepage ~ ChooseUniformly(possibilities[:homepage])
        jid ~ ChooseUniformly(possibilities[:jid])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Keyword begin
        keyword ~ ChooseUniformly(possibilities[:keyword])
        kid ~ ChooseUniformly(possibilities[:kid])
    end

    @class Organization begin
        continent ~ ChooseUniformly(possibilities[:continent])
        homepage ~ ChooseUniformly(possibilities[:homepage])
        name ~ ChooseUniformly(possibilities[:name])
        oid ~ ChooseUniformly(possibilities[:oid])
    end

    @class Obs begin
        author ~ Author
        conference ~ Conference
        domain ~ Domain
        journal ~ Journal
        keyword ~ Keyword
        organization ~ Organization
        abstract ~ ChooseUniformly(possibilities[:abstract])
        citation_num ~ ChooseUniformly(possibilities[:citation_num])
        pid ~ ChooseUniformly(possibilities[:pid])
        reference_num ~ ChooseUniformly(possibilities[:reference_num])
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
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
    publication_abstract abstract
    publication_citation_num citation_num
    publication_pid pid
    publication_reference_num reference_num
    publication_title title
    publication_year year
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

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
