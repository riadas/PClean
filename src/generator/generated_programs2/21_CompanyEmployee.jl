using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("people_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("people_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "people id"], Any[0, "age"], Any[0, "name"], Any[0, "nationality"], Any[0, "graduation college"], Any[1, "company id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales in billion"], Any[1, "profits in billion"], Any[1, "assets in billion"], Any[1, "market value in billion"], Any[2, "company id"], Any[2, "people id"], Any[2, "year working"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "people id"], Any[0, "age"], Any[0, "name"], Any[0, "nationality"], Any[0, "graduation college"], Any[1, "company id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales in billion"], Any[1, "profits in billion"], Any[1, "assets in billion"], Any[1, "market value in billion"], Any[2, "company id"], Any[2, "people id"], Any[2, "year working"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["people id", "company id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "age"], Any[0, "name"], Any[0, "nationality"], Any[0, "graduation college"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales in billion"], Any[1, "profits in billion"], Any[1, "assets in billion"], Any[1, "market value in billion"], Any[2, "year working"]]
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





PClean.@model CompanyEmployeeModel begin
    @class People begin
        people_id ~ Unmodeled()
        age ~ ChooseUniformly(possibilities[:age])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        graduation_college ~ ChooseUniformly(possibilities[:graduation_college])
    end

    @class Company begin
        company_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        headquarters ~ ChooseUniformly(possibilities[:headquarters])
        industry ~ ChooseUniformly(possibilities[:industry])
        sales_in_billion ~ ChooseUniformly(possibilities[:sales_in_billion])
        profits_in_billion ~ ChooseUniformly(possibilities[:profits_in_billion])
        assets_in_billion ~ ChooseUniformly(possibilities[:assets_in_billion])
        market_value_in_billion ~ ChooseUniformly(possibilities[:market_value_in_billion])
    end

    @class Employment begin
        company ~ Company
        people ~ People
        year_working ~ ChooseUniformly(possibilities[:year_working])
    end

    @class Obs begin
        employment ~ Employment
    end
end

query = @query CompanyEmployeeModel.Obs [
    people_id employment.people.people_id
    people_age employment.people.age
    people_name employment.people.name
    people_nationality employment.people.nationality
    people_graduation_college employment.people.graduation_college
    company_id employment.company.company_id
    company_name employment.company.name
    company_headquarters employment.company.headquarters
    company_industry employment.company.industry
    company_sales_in_billion employment.company.sales_in_billion
    company_profits_in_billion employment.company.profits_in_billion
    company_assets_in_billion employment.company.assets_in_billion
    company_market_value_in_billion employment.company.market_value_in_billion
    employment_year_working employment.year_working
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))