using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("buildings_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("buildings_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "city"], Any[0, "height"], Any[0, "stories"], Any[0, "status"], Any[1, "id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales billion"], Any[1, "profits billion"], Any[1, "assets billion"], Any[1, "market value billion"], Any[2, "building id"], Any[2, "company id"], Any[2, "move in year"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "city"], Any[0, "height"], Any[0, "stories"], Any[0, "status"], Any[1, "id"], Any[1, "name"], Any[1, "headquarters"], Any[1, "industry"], Any[1, "sales billion"], Any[1, "profits billion"], Any[1, "assets billion"], Any[1, "market value billion"], Any[2, "building id"], Any[2, "company id"], Any[2, "move in year"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Office_Locations begin
        building_id ~ Unmodeled()
        company_id ~ ChooseUniformly(possibilities[:company_id])
        move_in_year ~ ChooseUniformly(possibilities[:move_in_year])
    end

    @class Obs begin
        buildings ~ Buildings
        companies ~ Companies
        office_Locations ~ Office_Locations
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
    office_locations_move_in_year office_Locations.move_in_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
