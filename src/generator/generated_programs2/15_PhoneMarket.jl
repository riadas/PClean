using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("phone_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("phone_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "name"], Any[0, "phone id"], Any[0, "memory in g"], Any[0, "carrier"], Any[0, "price"], Any[1, "market id"], Any[1, "district"], Any[1, "num of employees"], Any[1, "num of shops"], Any[1, "ranking"], Any[2, "market id"], Any[2, "phone id"], Any[2, "num of stock"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "name"], Any[0, "phone id"], Any[0, "memory in g"], Any[0, "carrier"], Any[0, "price"], Any[1, "market id"], Any[1, "district"], Any[1, "num of employees"], Any[1, "num of shops"], Any[1, "ranking"], Any[2, "market id"], Any[2, "phone id"], Any[2, "num of stock"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "name"], Any[0, "phone id"], Any[0, "memory in g"], Any[0, "carrier"], Any[0, "price"], Any[1, "market id"], Any[1, "district"], Any[1, "num of employees"], Any[1, "num of shops"], Any[1, "ranking"], Any[2, "market id"], Any[2, "phone id"], Any[2, "num of stock"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "name"], Any[0, "phone id"], Any[0, "memory in g"], Any[0, "carrier"], Any[0, "price"], Any[1, "market id"], Any[1, "district"], Any[1, "num of employees"], Any[1, "num of shops"], Any[1, "ranking"], Any[2, "market id"], Any[2, "phone id"], Any[2, "num of stock"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "name"], Any[0, "phone id"], Any[0, "memory in g"], Any[0, "carrier"], Any[0, "price"], Any[1, "market id"], Any[1, "district"], Any[1, "num of employees"], Any[1, "num of shops"], Any[1, "ranking"], Any[2, "market id"], Any[2, "phone id"], Any[2, "num of stock"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 2], Any[11, 6]])
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







PClean.@model PhoneMarketModel begin
    @class Phone begin
        name ~ ChooseUniformly(possibilities[:name])
        memory_in_g ~ ChooseUniformly(possibilities[:memory_in_g])
        carrier ~ ChooseUniformly(possibilities[:carrier])
        price ~ ChooseUniformly(possibilities[:price])
    end

    @class Market begin
        district ~ ChooseUniformly(possibilities[:district])
        num_of_employees ~ ChooseUniformly(possibilities[:num_of_employees])
        num_of_shops ~ ChooseUniformly(possibilities[:num_of_shops])
        ranking ~ ChooseUniformly(possibilities[:ranking])
    end

    @class Phone_market begin
        phone ~ Phone
        num_of_stock ~ ChooseUniformly(possibilities[:num_of_stock])
    end

    @class Obs begin
        phone_market ~ Phone_market
    end
end

query = @query PhoneMarketModel.Obs [
    phone_name phone_market.phone.name
    phone_id phone_market.phone.phone_id
    phone_memory_in_g phone_market.phone.memory_in_g
    phone_carrier phone_market.phone.carrier
    phone_price phone_market.phone.price
    market_id phone_market.market.market_id
    market_district phone_market.market.district
    market_num_of_employees phone_market.market.num_of_employees
    market_num_of_shops phone_market.market.num_of_shops
    market_ranking phone_market.market.ranking
    phone_market_num_of_stock phone_market.num_of_stock
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
