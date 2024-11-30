using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("web client accelerator_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("web client accelerator_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "operating system"], Any[0, "client"], Any[0, "connection"], Any[1, "id"], Any[1, "name"], Any[1, "market share"], Any[2, "accelerator id"], Any[2, "browser id"], Any[2, "compatible since year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "operating system"], Any[0, "client"], Any[0, "connection"], Any[1, "id"], Any[1, "name"], Any[1, "market share"], Any[2, "accelerator id"], Any[2, "browser id"], Any[2, "compatible since year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["browser id", "accelerator id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "operating system"], Any[0, "client"], Any[0, "connection"], Any[1, "id"], Any[1, "name"], Any[1, "market share"], Any[2, "compatible since year"]]
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
        web_Client_Accelerator ~ Web_Client_Accelerator
        browser ~ Browser
        compatible_since_year ~ ChooseUniformly(possibilities[:compatible_since_year])
    end

    @class Obs begin
        accelerator_Compatible_Browser ~ Accelerator_Compatible_Browser
    end
end

query = @query BrowserWebModel.Obs [
    web_client_accelerator_id accelerator_Compatible_Browser.web_Client_Accelerator.id
    web_client_accelerator_name accelerator_Compatible_Browser.web_Client_Accelerator.name
    web_client_accelerator_operating_system accelerator_Compatible_Browser.web_Client_Accelerator.operating_system
    web_client_accelerator_client accelerator_Compatible_Browser.web_Client_Accelerator.client
    web_client_accelerator_connection accelerator_Compatible_Browser.web_Client_Accelerator.connection
    browser_id accelerator_Compatible_Browser.browser.id
    browser_name accelerator_Compatible_Browser.browser.name
    browser_market_share accelerator_Compatible_Browser.browser.market_share
    accelerator_compatible_browser_compatible_since_year accelerator_Compatible_Browser.compatible_since_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
