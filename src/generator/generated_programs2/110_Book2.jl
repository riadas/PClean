using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("publication_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("publication_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "book id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "book id"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "book id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "book id"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "book id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "book id"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "book id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "book id"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "book id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "book id"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[2, 6]])
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







PClean.@model Book2Model begin
    @class Book begin
        title ~ ChooseUniformly(possibilities[:title])
        issues ~ ChooseUniformly(possibilities[:issues])
        writer ~ ChooseUniformly(possibilities[:writer])
    end

    @class Publication begin
        book ~ Book
        publisher ~ ChooseUniformly(possibilities[:publisher])
        publication_date ~ ChooseUniformly(possibilities[:publication_date])
        price ~ ChooseUniformly(possibilities[:price])
    end

    @class Obs begin
        publication ~ Publication
    end
end

query = @query Book2Model.Obs [
    publication_id publication.publication_id
    publication_publisher publication.publisher
    publication_date publication.publication_date
    publication_price publication.price
    book_id publication.book.book_id
    book_title publication.book.title
    book_issues publication.book.issues
    book_writer publication.book.writer
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
