using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("publication_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("publication_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["book id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "publication id"], Any[0, "publisher"], Any[0, "publication date"], Any[0, "price"], Any[1, "title"], Any[1, "issues"], Any[1, "writer"]]
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





PClean.@model Book2Model begin
    @class Book begin
        book_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        issues ~ ChooseUniformly(possibilities[:issues])
        writer ~ ChooseUniformly(possibilities[:writer])
    end

    @class Obs begin
        book ~ Book
        publication_id ~ Unmodeled()
        publisher ~ ChooseUniformly(possibilities[:publisher])
        publication_date ~ ChooseUniformly(possibilities[:publication_date])
        price ~ ChooseUniformly(possibilities[:price])
    end
end

query = @query Book2Model.Obs [
    publication_id publication_id
    publication_publisher publisher
    publication_date publication_date
    publication_price price
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

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
