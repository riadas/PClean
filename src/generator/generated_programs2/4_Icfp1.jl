using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("institution_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("institution_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "institution id"], Any[0, "name"], Any[0, "country"], Any[1, "author id"], Any[1, "last name"], Any[1, "first name"], Any[2, "paper id"], Any[2, "title"], Any[3, "author id"], Any[3, "institution id"], Any[3, "paper id"], Any[3, "author count"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "institution id"], Any[0, "name"], Any[0, "country"], Any[1, "author id"], Any[1, "last name"], Any[1, "first name"], Any[2, "paper id"], Any[2, "title"], Any[3, "author id"], Any[3, "institution id"], Any[3, "paper id"], Any[3, "author count"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["paper id", "institution id", "author id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "country"], Any[1, "last name"], Any[1, "first name"], Any[2, "title"], Any[3, "author count"]]
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





PClean.@model Icfp1Model begin
    @class Institution begin
        institution_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Authors begin
        author_id ~ Unmodeled()
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
    end

    @class Papers begin
        paper_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
    end

    @class Authorship_Count begin
        authors ~ Authors
        institution ~ Institution
        papers ~ Papers
        author_count ~ ChooseUniformly(possibilities[:author_count])
    end

    @class Obs begin
        authorship_Count ~ Authorship_Count
    end
end

query = @query Icfp1Model.Obs [
    institution_id authorship_Count.institution.institution_id
    institution_name authorship_Count.institution.name
    institution_country authorship_Count.institution.country
    authors_author_id authorship_Count.authors.author_id
    authors_last_name authorship_Count.authors.last_name
    authors_first_name authorship_Count.authors.first_name
    papers_paper_id authorship_Count.papers.paper_id
    papers_title authorship_Count.papers.title
    authorship_count_author_count authorship_Count.author_count
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
