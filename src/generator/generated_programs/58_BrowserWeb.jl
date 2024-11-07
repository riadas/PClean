using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("web client accelerator_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("web client accelerator_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "operating system"], Any[0, "client"], Any[0, "connection"], Any[1, "id"], Any[1, "name"], Any[1, "market share"], Any[2, "accelerator id"], Any[2, "browser id"], Any[2, "compatible since year"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "operating system"], Any[0, "client"], Any[0, "connection"], Any[1, "id"], Any[1, "name"], Any[1, "market share"], Any[2, "accelerator id"], Any[2, "browser id"], Any[2, "compatible since year"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model BrowserWebModel begin
    @class Web_Client_Accelerator begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        operating_system ~ ChooseUniformly(possibilities[:operating_system])
        client ~ ChooseUniformly(possibilities[:client])
        connection ~ ChooseUniformly(possibilities[:connection])
    end

    @class Browser begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        market_share ~ ChooseUniformly(possibilities[:market_share])
    end

    @class Accelerator_Compatible_Browser begin
        accelerator_id ~ Unmodeled()
        browser_id ~ ChooseUniformly(possibilities[:browser_id])
        compatible_since_year ~ ChooseUniformly(possibilities[:compatible_since_year])
    end

    @class Obs begin
        web_Client_Accelerator ~ Web_Client_Accelerator
        browser ~ Browser
        accelerator_Compatible_Browser ~ Accelerator_Compatible_Browser
    end
end

query = @query BrowserWebModel.Obs [
    web_client_accelerator_id web_Client_Accelerator.id
    web_client_accelerator_name web_Client_Accelerator.name
    web_client_accelerator_operating_system web_Client_Accelerator.operating_system
    web_client_accelerator_client web_Client_Accelerator.client
    web_client_accelerator_connection web_Client_Accelerator.connection
    browser_id browser.id
    browser_name browser.name
    browser_market_share browser.market_share
    accelerator_compatible_browser_compatible_since_year accelerator_Compatible_Browser.compatible_since_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
