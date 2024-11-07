using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("book club_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("book club_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "book club id"], Any[0, "year"], Any[0, "author or editor"], Any[0, "book title"], Any[0, "publisher"], Any[0, "category"], Any[0, "result"], Any[1, "movie id"], Any[1, "title"], Any[1, "year"], Any[1, "director"], Any[1, "budget million"], Any[1, "gross worldwide"], Any[2, "company name"], Any[2, "type"], Any[2, "incorporated in"], Any[2, "group equity shareholding"], Any[2, "book club id"], Any[2, "movie id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "book club id"], Any[0, "year"], Any[0, "author or editor"], Any[0, "book title"], Any[0, "publisher"], Any[0, "category"], Any[0, "result"], Any[1, "movie id"], Any[1, "title"], Any[1, "year"], Any[1, "director"], Any[1, "budget million"], Any[1, "gross worldwide"], Any[2, "company name"], Any[2, "type"], Any[2, "incorporated in"], Any[2, "group equity shareholding"], Any[2, "book club id"], Any[2, "movie id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Culture_Company begin
        company_name ~ ChooseUniformly(possibilities[:company_name])
        type ~ ChooseUniformly(possibilities[:type])
        incorporated_in ~ ChooseUniformly(possibilities[:incorporated_in])
        group_equity_shareholding ~ ChooseUniformly(possibilities[:group_equity_shareholding])
        book_club_id ~ ChooseUniformly(possibilities[:book_club_id])
        movie_id ~ ChooseUniformly(possibilities[:movie_id])
    end

    @class Obs begin
        book_Club ~ Book_Club
        movie ~ Movie
        culture_Company ~ Culture_Company
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
    culture_company_company_name culture_Company.company_name
    culture_company_type culture_Company.type
    culture_company_incorporated_in culture_Company.incorporated_in
    culture_company_group_equity_shareholding culture_Company.group_equity_shareholding
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
