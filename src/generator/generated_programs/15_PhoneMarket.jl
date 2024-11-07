using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("phone_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("phone_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "name"], Any[0, "phone id"], Any[0, "memory in g"], Any[0, "carrier"], Any[0, "price"], Any[1, "market id"], Any[1, "district"], Any[1, "num of employees"], Any[1, "num of shops"], Any[1, "ranking"], Any[2, "market id"], Any[2, "phone id"], Any[2, "num of stock"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "name"], Any[0, "phone id"], Any[0, "memory in g"], Any[0, "carrier"], Any[0, "price"], Any[1, "market id"], Any[1, "district"], Any[1, "num of employees"], Any[1, "num of shops"], Any[1, "ranking"], Any[2, "market id"], Any[2, "phone id"], Any[2, "num of stock"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model PhoneMarketModel begin
    @class Phone begin
        name ~ ChooseUniformly(possibilities[:name])
        phone_id ~ ChooseUniformly(possibilities[:phone_id])
        memory_in_g ~ ChooseUniformly(possibilities[:memory_in_g])
        carrier ~ ChooseUniformly(possibilities[:carrier])
        price ~ ChooseUniformly(possibilities[:price])
    end

    @class Market begin
        market_id ~ Unmodeled()
        district ~ ChooseUniformly(possibilities[:district])
        num_of_employees ~ ChooseUniformly(possibilities[:num_of_employees])
        num_of_shops ~ ChooseUniformly(possibilities[:num_of_shops])
        ranking ~ ChooseUniformly(possibilities[:ranking])
    end

    @class Phone_Market begin
        market_id ~ Unmodeled()
        phone_id ~ ChooseUniformly(possibilities[:phone_id])
        num_of_stock ~ ChooseUniformly(possibilities[:num_of_stock])
    end

    @class Obs begin
        phone ~ Phone
        market ~ Market
        phone_Market ~ Phone_Market
    end
end

query = @query PhoneMarketModel.Obs [
    phone_name phone.name
    phone_id phone.phone_id
    phone_memory_in_g phone.memory_in_g
    phone_carrier phone.carrier
    phone_price phone.price
    market_id market.market_id
    market_district market.district
    market_num_of_employees market.num_of_employees
    market_num_of_shops market.num_of_shops
    market_ranking market.ranking
    phone_market_num_of_stock phone_Market.num_of_stock
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
