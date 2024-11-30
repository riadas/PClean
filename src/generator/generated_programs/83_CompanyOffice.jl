using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("buildings_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("buildings_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "city"], Any[0, "height"], Any[0, "stories"], Any[0, "status"], Any[1, "id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales billion"], Any[1, "profits billion"], Any[1, "assets billion"], Any[1, "market value billion"], Any[2, "building id"], Any[2, "company id"], Any[2, "move in year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "city"], Any[0, "height"], Any[0, "stories"], Any[0, "status"], Any[1, "id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales billion"], Any[1, "profits billion"], Any[1, "assets billion"], Any[1, "market value billion"], Any[2, "building id"], Any[2, "company id"], Any[2, "move in year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["company id", "building id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "city"], Any[0, "height"], Any[0, "stories"], Any[0, "status"], Any[1, "id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales billion"], Any[1, "profits billion"], Any[1, "assets billion"], Any[1, "market value billion"], Any[2, "move in year"]]
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





PClean.@model CompanyOfficeModel begin
    @class Buildings begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        city ~ ChooseUniformly(possibilities[:city])
        height ~ ChooseUniformly(possibilities[:height])
        stories ~ ChooseUniformly(possibilities[:stories])
        status ~ ChooseUniformly(possibilities[:status])
    end

    @class Companies begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        headquarters ~ ChooseUniformly(possibilities[:headquarters])
        industry ~ ChooseUniformly(possibilities[:industry])
        sales_billion ~ ChooseUniformly(possibilities[:sales_billion])
        profits_billion ~ ChooseUniformly(possibilities[:profits_billion])
        assets_billion ~ ChooseUniformly(possibilities[:assets_billion])
        market_value_billion ~ ChooseUniformly(possibilities[:market_value_billion])
    end

    @class Obs begin
        buildings ~ Buildings
        companies ~ Companies
        move_in_year ~ ChooseUniformly(possibilities[:move_in_year])
    end
end

query = @query CompanyOfficeModel.Obs [
    buildings_id buildings.id
    buildings_name buildings.name
    buildings_city buildings.city
    buildings_height buildings.height
    buildings_stories buildings.stories
    buildings_status buildings.status
    companies_id companies.id
    companies_name companies.name
    companies_headquarters companies.headquarters
    companies_industry companies.industry
    companies_sales_billion companies.sales_billion
    companies_profits_billion companies.profits_billion
    companies_assets_billion companies.assets_billion
    companies_market_value_billion companies.market_value_billion
    office_locations_move_in_year move_in_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
