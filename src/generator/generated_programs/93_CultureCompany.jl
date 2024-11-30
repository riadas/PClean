using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("book club_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("book club_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "book club id"], Any[0, "year"], Any[0, "author or editor"], Any[0, "book title"], Any[0, "publisher"], Any[0, "category"], Any[0, "result"], Any[1, "movie id"], Any[1, "title"], Any[1, "year"], Any[1, "director"], Any[1, "budget million"], Any[1, "gross worldwide"], Any[2, "company name"], Any[2, "type"], Any[2, "incorporated in"], Any[2, "group equity shareholding"], Any[2, "book club id"], Any[2, "movie id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "book club id"], Any[0, "year"], Any[0, "author or editor"], Any[0, "book title"], Any[0, "publisher"], Any[0, "category"], Any[0, "result"], Any[1, "movie id"], Any[1, "title"], Any[1, "year"], Any[1, "director"], Any[1, "budget million"], Any[1, "gross worldwide"], Any[2, "company name"], Any[2, "type"], Any[2, "incorporated in"], Any[2, "group equity shareholding"], Any[2, "book club id"], Any[2, "movie id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["movie id", "book club id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "year"], Any[0, "author or editor"], Any[0, "book title"], Any[0, "publisher"], Any[0, "category"], Any[0, "result"], Any[1, "title"], Any[1, "year"], Any[1, "director"], Any[1, "budget million"], Any[1, "gross worldwide"], Any[2, "company name"], Any[2, "type"], Any[2, "incorporated in"], Any[2, "group equity shareholding"]]
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





PClean.@model CultureCompanyModel begin
    @class Book_Club begin
        book_club_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        author_or_editor ~ ChooseUniformly(possibilities[:author_or_editor])
        book_title ~ ChooseUniformly(possibilities[:book_title])
        publisher ~ ChooseUniformly(possibilities[:publisher])
        category ~ ChooseUniformly(possibilities[:category])
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Movie begin
        movie_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
        director ~ ChooseUniformly(possibilities[:director])
        budget_million ~ ChooseUniformly(possibilities[:budget_million])
        gross_worldwide ~ ChooseUniformly(possibilities[:gross_worldwide])
    end

    @class Obs begin
        book_Club ~ Book_Club
        movie ~ Movie
        company_name ~ ChooseUniformly(possibilities[:company_name])
        type ~ ChooseUniformly(possibilities[:type])
        incorporated_in ~ ChooseUniformly(possibilities[:incorporated_in])
        group_equity_shareholding ~ ChooseUniformly(possibilities[:group_equity_shareholding])
    end
end

query = @query CultureCompanyModel.Obs [
    book_club_id book_Club.book_club_id
    book_club_year book_Club.year
    book_club_author_or_editor book_Club.author_or_editor
    book_club_book_title book_Club.book_title
    book_club_publisher book_Club.publisher
    book_club_category book_Club.category
    book_club_result book_Club.result
    movie_id movie.movie_id
    movie_title movie.title
    movie_year movie.year
    movie_director movie.director
    movie_budget_million movie.budget_million
    movie_gross_worldwide movie.gross_worldwide
    culture_company_company_name company_name
    culture_company_type type
    culture_company_incorporated_in incorporated_in
    culture_company_group_equity_shareholding group_equity_shareholding
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
