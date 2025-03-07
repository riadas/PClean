using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("shop_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("shop_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "shop id"], Any[0, "address"], Any[0, "num of staff"], Any[0, "score"], Any[0, "open year"], Any[1, "member id"], Any[1, "name"], Any[1, "membership card"], Any[1, "age"], Any[1, "time of purchase"], Any[1, "level of membership"], Any[1, "address"], Any[2, "hh id"], Any[2, "shop id"], Any[2, "month"], Any[2, "num of shaff in charge"], Any[3, "hh id"], Any[3, "member id"], Any[3, "total amount"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "shop id"], Any[0, "address"], Any[0, "num of staff"], Any[0, "score"], Any[0, "open year"], Any[1, "member id"], Any[1, "name"], Any[1, "membership card"], Any[1, "age"], Any[1, "time of purchase"], Any[1, "level of membership"], Any[1, "address"], Any[2, "hh id"], Any[2, "shop id"], Any[2, "month"], Any[2, "num of shaff in charge"], Any[3, "hh id"], Any[3, "member id"], Any[3, "total amount"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "shop id"], Any[0, "address"], Any[0, "num of staff"], Any[0, "score"], Any[0, "open year"], Any[1, "member id"], Any[1, "name"], Any[1, "membership card"], Any[1, "age"], Any[1, "time of purchase"], Any[1, "level of membership"], Any[1, "address"], Any[2, "hh id"], Any[2, "shop id"], Any[2, "month"], Any[2, "num of shaff in charge"], Any[3, "hh id"], Any[3, "member id"], Any[3, "total amount"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "shop id"], Any[0, "address"], Any[0, "num of staff"], Any[0, "score"], Any[0, "open year"], Any[1, "member id"], Any[1, "name"], Any[1, "membership card"], Any[1, "age"], Any[1, "time of purchase"], Any[1, "level of membership"], Any[1, "address"], Any[2, "hh id"], Any[2, "shop id"], Any[2, "month"], Any[2, "num of shaff in charge"], Any[3, "hh id"], Any[3, "member id"], Any[3, "total amount"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "shop id"], Any[0, "address"], Any[0, "num of staff"], Any[0, "score"], Any[0, "open year"], Any[1, "member id"], Any[1, "name"], Any[1, "membership card"], Any[1, "age"], Any[1, "time of purchase"], Any[1, "level of membership"], Any[1, "address"], Any[2, "hh id"], Any[2, "shop id"], Any[2, "month"], Any[2, "num of shaff in charge"], Any[3, "hh id"], Any[3, "member id"], Any[3, "total amount"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[14, 1], Any[18, 6]])
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







PClean.@model CoffeeShopModel begin
    @class Shop begin
        address ~ ChooseUniformly(possibilities[:address])
        num_of_staff ~ ChooseUniformly(possibilities[:num_of_staff])
        score ~ ChooseUniformly(possibilities[:score])
        open_year ~ ChooseUniformly(possibilities[:open_year])
    end

    @class Member begin
        name ~ ChooseUniformly(possibilities[:name])
        membership_card ~ ChooseUniformly(possibilities[:membership_card])
        age ~ ChooseUniformly(possibilities[:age])
        time_of_purchase ~ ChooseUniformly(possibilities[:time_of_purchase])
        level_of_membership ~ ChooseUniformly(possibilities[:level_of_membership])
        address ~ ChooseUniformly(possibilities[:address])
    end

    @class Happy_hour begin
        shop ~ Shop
        month ~ ChooseUniformly(possibilities[:month])
        num_of_shaff_in_charge ~ ChooseUniformly(possibilities[:num_of_shaff_in_charge])
    end

    @class Happy_hour_member begin
        member ~ Member
        total_amount ~ ChooseUniformly(possibilities[:total_amount])
    end

    @class Obs begin
        happy_hour ~ Happy_hour
        happy_hour_member ~ Happy_hour_member
    end
end

query = @query CoffeeShopModel.Obs [
    shop_id happy_hour.shop.shop_id
    shop_address happy_hour.shop.address
    shop_num_of_staff happy_hour.shop.num_of_staff
    shop_score happy_hour.shop.score
    shop_open_year happy_hour.shop.open_year
    member_id happy_hour_member.member.member_id
    member_name happy_hour_member.member.name
    membership_card happy_hour_member.member.membership_card
    member_age happy_hour_member.member.age
    member_time_of_purchase happy_hour_member.member.time_of_purchase
    level_of_membership happy_hour_member.member.level_of_membership
    member_address happy_hour_member.member.address
    happy_hour_hh_id happy_hour.hh_id
    happy_hour_month happy_hour.month
    happy_hour_num_of_shaff_in_charge happy_hour.num_of_shaff_in_charge
    happy_hour_member_hh_id happy_hour_member.hh_id
    happy_hour_member_total_amount happy_hour_member.total_amount
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
