using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("buildings_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("buildings_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "city"], Any[0, "height"], Any[0, "stories"], Any[0, "status"], Any[1, "id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales billion"], Any[1, "profits billion"], Any[1, "assets billion"], Any[1, "market value billion"], Any[2, "building id"], Any[2, "company id"], Any[2, "move in year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[16, 7], Any[15, 1]])
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







PClean.@model CompanyOfficeModel begin
    @class Buildings begin
        name ~ ChooseUniformly(possibilities[:name])
        city ~ ChooseUniformly(possibilities[:city])
        height ~ ChooseUniformly(possibilities[:height])
        stories ~ ChooseUniformly(possibilities[:stories])
        status ~ ChooseUniformly(possibilities[:status])
    end

    @class Companies begin
        name ~ ChooseUniformly(possibilities[:name])
        headquarters ~ ChooseUniformly(possibilities[:headquarters])
        industry ~ ChooseUniformly(possibilities[:industry])
        sales_billion ~ ChooseUniformly(possibilities[:sales_billion])
        profits_billion ~ ChooseUniformly(possibilities[:profits_billion])
        assets_billion ~ ChooseUniformly(possibilities[:assets_billion])
        market_value_billion ~ ChooseUniformly(possibilities[:market_value_billion])
    end

    @class Office_locations begin
        companies ~ Companies
        move_in_year ~ ChooseUniformly(possibilities[:move_in_year])
    end

    @class Obs begin
        office_locations ~ Office_locations
    end
end

query = @query CompanyOfficeModel.Obs [
    buildings_name office_locations.buildings.name
    buildings_city office_locations.buildings.city
    buildings_height office_locations.buildings.height
    buildings_stories office_locations.buildings.stories
    buildings_status office_locations.buildings.status
    companies_name office_locations.companies.name
    companies_headquarters office_locations.companies.headquarters
    companies_industry office_locations.companies.industry
    companies_sales_billion office_locations.companies.sales_billion
    companies_profits_billion office_locations.companies.profits_billion
    companies_assets_billion office_locations.companies.assets_billion
    companies_market_value_billion office_locations.companies.market_value_billion
    office_locations_move_in_year office_locations.move_in_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
