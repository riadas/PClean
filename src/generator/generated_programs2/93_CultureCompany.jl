using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("book_club_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("book_club_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "book club id"], Any[0, "year"], Any[0, "author or editor"], Any[0, "book title"], Any[0, "publisher"], Any[0, "category"], Any[0, "result"], Any[1, "movie id"], Any[1, "title"], Any[1, "year"], Any[1, "director"], Any[1, "budget million"], Any[1, "gross worldwide"], Any[2, "company name"], Any[2, "type"], Any[2, "incorporated in"], Any[2, "group equity shareholding"], Any[2, "book club id"], Any[2, "movie id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[19, 8], Any[18, 1]])
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







PClean.@model CultureCompanyModel begin
    @class Book_club begin
        year ~ ChooseUniformly(possibilities[:year])
        author_or_editor ~ ChooseUniformly(possibilities[:author_or_editor])
        book_title ~ ChooseUniformly(possibilities[:book_title])
        publisher ~ ChooseUniformly(possibilities[:publisher])
        category ~ ChooseUniformly(possibilities[:category])
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Movie begin
        title ~ ChooseUniformly(possibilities[:title])
        year ~ ChooseUniformly(possibilities[:year])
        director ~ ChooseUniformly(possibilities[:director])
        budget_million ~ ChooseUniformly(possibilities[:budget_million])
        gross_worldwide ~ ChooseUniformly(possibilities[:gross_worldwide])
    end

    @class Culture_company begin
        company_name ~ ChooseUniformly(possibilities[:company_name])
        type ~ ChooseUniformly(possibilities[:type])
        incorporated_in ~ ChooseUniformly(possibilities[:incorporated_in])
        group_equity_shareholding ~ ChooseUniformly(possibilities[:group_equity_shareholding])
        book_club ~ Book_club
        movie ~ Movie
    end

    @class Obs begin
        culture_company ~ Culture_company
    end
end

query = @query CultureCompanyModel.Obs [
    book_club_id culture_company.book_club.book_club_id
    book_club_year culture_company.book_club.year
    book_club_author_or_editor culture_company.book_club.author_or_editor
    book_club_book_title culture_company.book_club.book_title
    book_club_publisher culture_company.book_club.publisher
    book_club_category culture_company.book_club.category
    book_club_result culture_company.book_club.result
    movie_id culture_company.movie.movie_id
    movie_title culture_company.movie.title
    movie_year culture_company.movie.year
    movie_director culture_company.movie.director
    movie_budget_million culture_company.movie.budget_million
    movie_gross_worldwide culture_company.movie.gross_worldwide
    culture_company_company_name culture_company.company_name
    culture_company_type culture_company.type
    culture_company_incorporated_in culture_company.incorporated_in
    culture_company_group_equity_shareholding culture_company.group_equity_shareholding
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
