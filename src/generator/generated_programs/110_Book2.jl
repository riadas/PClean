using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("publication_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("publication_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "book id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "book id"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "book id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "book id"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Book2Model begin
    @class Publication begin
        publication_id ~ Unmodeled()
        book_id ~ ChooseUniformly(possibilities[:book_id])
        publisher ~ ChooseUniformly(possibilities[:publisher])
        publication_date ~ ChooseUniformly(possibilities[:publication_date])
        price ~ ChooseUniformly(possibilities[:price])
    end

    @class Book begin
        book_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        issues ~ ChooseUniformly(possibilities[:issues])
        writer ~ ChooseUniformly(possibilities[:writer])
    end

    @class Obs begin
        publication ~ Publication
        book ~ Book
    end
end

query = @query Book2Model.Obs [
    publication_id publication.publication_id
    publication_publisher publication.publisher
    publication_date publication.publication_date
    publication_price publication.price
    book_id book.book_id
    book_title book.title
    book_issues book.issues
    book_writer book.writer
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
