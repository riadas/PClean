using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("accounts_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("accounts_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "name"], Any[1, "customer id"], Any[1, "balance"], Any[2, "customer id"], Any[2, "balance"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "name"], Any[1, "customer id"], Any[1, "balance"], Any[2, "customer id"], Any[2, "balance"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "name"], Any[1, "customer id"], Any[1, "balance"], Any[2, "customer id"], Any[2, "balance"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "name"], Any[1, "customer id"], Any[1, "balance"], Any[2, "customer id"], Any[2, "balance"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "name"], Any[1, "customer id"], Any[1, "balance"], Any[2, "customer id"], Any[2, "balance"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[3, 1], Any[5, 1]])
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







PClean.@model SmallBank1Model begin
    @class Accounts begin
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Savings begin
        balance ~ ChooseUniformly(possibilities[:balance])
    end

    @class Checking begin
        balance ~ ChooseUniformly(possibilities[:balance])
    end

    @class Obs begin
        savings ~ Savings
        checking ~ Checking
    end
end

query = @query SmallBank1Model.Obs [
    accounts_customer_id savings.accounts.customer_id
    accounts_name savings.accounts.name
    savings_balance savings.balance
    checking_balance checking.balance
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
