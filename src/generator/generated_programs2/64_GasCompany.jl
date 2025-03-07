using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("company_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("company_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "company id"], Any[0, "rank"], Any[0, "company"], Any[0, "headquarters"], Any[0, "main industry"], Any[0, "sales billion"], Any[0, "profits billion"], Any[0, "assets billion"], Any[0, "market value"], Any[1, "station id"], Any[1, "open year"], Any[1, "location"], Any[1, "manager name"], Any[1, "vice manager name"], Any[1, "representative name"], Any[2, "station id"], Any[2, "company id"], Any[2, "rank of the year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "company id"], Any[0, "rank"], Any[0, "company"], Any[0, "headquarters"], Any[0, "main industry"], Any[0, "sales billion"], Any[0, "profits billion"], Any[0, "assets billion"], Any[0, "market value"], Any[1, "station id"], Any[1, "open year"], Any[1, "location"], Any[1, "manager name"], Any[1, "vice manager name"], Any[1, "representative name"], Any[2, "station id"], Any[2, "company id"], Any[2, "rank of the year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "company id"], Any[0, "rank"], Any[0, "company"], Any[0, "headquarters"], Any[0, "main industry"], Any[0, "sales billion"], Any[0, "profits billion"], Any[0, "assets billion"], Any[0, "market value"], Any[1, "station id"], Any[1, "open year"], Any[1, "location"], Any[1, "manager name"], Any[1, "vice manager name"], Any[1, "representative name"], Any[2, "station id"], Any[2, "company id"], Any[2, "rank of the year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "company id"], Any[0, "rank"], Any[0, "company"], Any[0, "headquarters"], Any[0, "main industry"], Any[0, "sales billion"], Any[0, "profits billion"], Any[0, "assets billion"], Any[0, "market value"], Any[1, "station id"], Any[1, "open year"], Any[1, "location"], Any[1, "manager name"], Any[1, "vice manager name"], Any[1, "representative name"], Any[2, "station id"], Any[2, "company id"], Any[2, "rank of the year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "company id"], Any[0, "rank"], Any[0, "company"], Any[0, "headquarters"], Any[0, "main industry"], Any[0, "sales billion"], Any[0, "profits billion"], Any[0, "assets billion"], Any[0, "market value"], Any[1, "station id"], Any[1, "open year"], Any[1, "location"], Any[1, "manager name"], Any[1, "vice manager name"], Any[1, "representative name"], Any[2, "station id"], Any[2, "company id"], Any[2, "rank of the year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[17, 1], Any[16, 10]])
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







PClean.@model GasCompanyModel begin
    @class Company begin
        rank ~ ChooseUniformly(possibilities[:rank])
        company ~ ChooseUniformly(possibilities[:company])
        headquarters ~ ChooseUniformly(possibilities[:headquarters])
        main_industry ~ ChooseUniformly(possibilities[:main_industry])
        sales_billion ~ ChooseUniformly(possibilities[:sales_billion])
        profits_billion ~ ChooseUniformly(possibilities[:profits_billion])
        assets_billion ~ ChooseUniformly(possibilities[:assets_billion])
        market_value ~ ChooseUniformly(possibilities[:market_value])
    end

    @class Gas_station begin
        open_year ~ ChooseUniformly(possibilities[:open_year])
        location ~ ChooseUniformly(possibilities[:location])
        manager_name ~ ChooseUniformly(possibilities[:manager_name])
        vice_manager_name ~ ChooseUniformly(possibilities[:vice_manager_name])
        representative_name ~ ChooseUniformly(possibilities[:representative_name])
    end

    @class Station_company begin
        company ~ Company
        rank_of_the_year ~ ChooseUniformly(possibilities[:rank_of_the_year])
    end

    @class Obs begin
        station_company ~ Station_company
    end
end

query = @query GasCompanyModel.Obs [
    company_id station_company.company.company_id
    company_rank station_company.company.rank
    company station_company.company.company
    company_headquarters station_company.company.headquarters
    company_main_industry station_company.company.main_industry
    company_sales_billion station_company.company.sales_billion
    company_profits_billion station_company.company.profits_billion
    company_assets_billion station_company.company.assets_billion
    company_market_value station_company.company.market_value
    gas_station_station_id station_company.gas_station.station_id
    gas_station_open_year station_company.gas_station.open_year
    gas_station_location station_company.gas_station.location
    gas_station_manager_name station_company.gas_station.manager_name
    gas_station_vice_manager_name station_company.gas_station.vice_manager_name
    gas_station_representative_name station_company.gas_station.representative_name
    station_company_rank_of_the_year station_company.rank_of_the_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
