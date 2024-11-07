using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("company_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("company_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "company id"], Any[0, "rank"], Any[0, "company"], Any[0, "headquarters"], Any[0, "main industry"], Any[0, "sales billion"], Any[0, "profits billion"], Any[0, "assets billion"], Any[0, "market value"], Any[1, "station id"], Any[1, "open year"], Any[1, "location"], Any[1, "manager name"], Any[1, "vice manager name"], Any[1, "representative name"], Any[2, "station id"], Any[2, "company id"], Any[2, "rank of the year"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "company id"], Any[0, "rank"], Any[0, "company"], Any[0, "headquarters"], Any[0, "main industry"], Any[0, "sales billion"], Any[0, "profits billion"], Any[0, "assets billion"], Any[0, "market value"], Any[1, "station id"], Any[1, "open year"], Any[1, "location"], Any[1, "manager name"], Any[1, "vice manager name"], Any[1, "representative name"], Any[2, "station id"], Any[2, "company id"], Any[2, "rank of the year"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model GasCompanyModel begin
    @class Company begin
        company_id ~ Unmodeled()
        rank ~ ChooseUniformly(possibilities[:rank])
        company ~ ChooseUniformly(possibilities[:company])
        headquarters ~ ChooseUniformly(possibilities[:headquarters])
        main_industry ~ ChooseUniformly(possibilities[:main_industry])
        sales_billion ~ ChooseUniformly(possibilities[:sales_billion])
        profits_billion ~ ChooseUniformly(possibilities[:profits_billion])
        assets_billion ~ ChooseUniformly(possibilities[:assets_billion])
        market_value ~ ChooseUniformly(possibilities[:market_value])
    end

    @class Gas_Station begin
        station_id ~ Unmodeled()
        open_year ~ ChooseUniformly(possibilities[:open_year])
        location ~ ChooseUniformly(possibilities[:location])
        manager_name ~ ChooseUniformly(possibilities[:manager_name])
        vice_manager_name ~ ChooseUniformly(possibilities[:vice_manager_name])
        representative_name ~ ChooseUniformly(possibilities[:representative_name])
    end

    @class Station_Company begin
        station_id ~ Unmodeled()
        company_id ~ ChooseUniformly(possibilities[:company_id])
        rank_of_the_year ~ ChooseUniformly(possibilities[:rank_of_the_year])
    end

    @class Obs begin
        company ~ Company
        gas_Station ~ Gas_Station
        station_Company ~ Station_Company
    end
end

query = @query GasCompanyModel.Obs [
    company_id company.company_id
    company_rank company.rank
    company company.company
    company_headquarters company.headquarters
    company_main_industry company.main_industry
    company_sales_billion company.sales_billion
    company_profits_billion company.profits_billion
    company_assets_billion company.assets_billion
    company_market_value company.market_value
    gas_station_station_id gas_Station.station_id
    gas_station_open_year gas_Station.open_year
    gas_station_location gas_Station.location
    gas_station_manager_name gas_Station.manager_name
    gas_station_vice_manager_name gas_Station.vice_manager_name
    gas_station_representative_name gas_Station.representative_name
    station_company_rank_of_the_year station_Company.rank_of_the_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
