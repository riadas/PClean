using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("web_client_accelerator_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("web_client_accelerator_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "operating system"], Any[0, "client"], Any[0, "connection"], Any[1, "id"], Any[1, "name"], Any[1, "market share"], Any[2, "accelerator id"], Any[2, "browser id"], Any[2, "compatible since year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[10, 6], Any[9, 1]])
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







PClean.@model BrowserWebModel begin
    @class Web_client_accelerator begin
        name ~ ChooseUniformly(possibilities[:name])
        operating_system ~ ChooseUniformly(possibilities[:operating_system])
        client ~ ChooseUniformly(possibilities[:client])
        connection ~ ChooseUniformly(possibilities[:connection])
    end

    @class Browser begin
        name ~ ChooseUniformly(possibilities[:name])
        market_share ~ ChooseUniformly(possibilities[:market_share])
    end

    @class Accelerator_compatible_browser begin
        browser ~ Browser
        compatible_since_year ~ ChooseUniformly(possibilities[:compatible_since_year])
    end

    @class Obs begin
        accelerator_compatible_browser ~ Accelerator_compatible_browser
    end
end

query = @query BrowserWebModel.Obs [
    web_client_accelerator_name accelerator_compatible_browser.web_client_accelerator.name
    web_client_accelerator_operating_system accelerator_compatible_browser.web_client_accelerator.operating_system
    web_client_accelerator_client accelerator_compatible_browser.web_client_accelerator.client
    web_client_accelerator_connection accelerator_compatible_browser.web_client_accelerator.connection
    browser_name accelerator_compatible_browser.browser.name
    browser_market_share accelerator_compatible_browser.browser.market_share
    accelerator_compatible_browser_compatible_since_year accelerator_compatible_browser.compatible_since_year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
