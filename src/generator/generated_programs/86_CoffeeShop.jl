using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("shop_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("shop_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["shop id", "member id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "address"], Any[0, "num of staff"], Any[0, "score"], Any[0, "open year"], Any[1, "name"], Any[1, "membership card"], Any[1, "age"], Any[1, "time of purchase"], Any[1, "level of membership"], Any[1, "address"], Any[2, "hh id"], Any[2, "month"], Any[2, "num of shaff in charge"], Any[3, "hh id"], Any[3, "total amount"]]
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





PClean.@model CoffeeShopModel begin
    @class Shop begin
        shop_id ~ Unmodeled()
        address ~ ChooseUniformly(possibilities[:address])
        num_of_staff ~ ChooseUniformly(possibilities[:num_of_staff])
        score ~ ChooseUniformly(possibilities[:score])
        open_year ~ ChooseUniformly(possibilities[:open_year])
    end

    @class Member begin
        member_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        membership_card ~ ChooseUniformly(possibilities[:membership_card])
        age ~ ChooseUniformly(possibilities[:age])
        time_of_purchase ~ ChooseUniformly(possibilities[:time_of_purchase])
        level_of_membership ~ ChooseUniformly(possibilities[:level_of_membership])
        address ~ ChooseUniformly(possibilities[:address])
    end

    @class Obs begin
        shop ~ Shop
        member ~ Member
        hh_id ~ Unmodeled()
        month ~ ChooseUniformly(possibilities[:month])
        num_of_shaff_in_charge ~ ChooseUniformly(possibilities[:num_of_shaff_in_charge])
        hh_id ~ Unmodeled()
        total_amount ~ ChooseUniformly(possibilities[:total_amount])
    end
end

query = @query CoffeeShopModel.Obs [
    shop_id shop.shop_id
    shop_address shop.address
    shop_num_of_staff shop.num_of_staff
    shop_score shop.score
    shop_open_year shop.open_year
    member_id member.member_id
    member_name member.name
    membership_card member.membership_card
    member_age member.age
    member_time_of_purchase member.time_of_purchase
    level_of_membership member.level_of_membership
    member_address member.address
    happy_hour_hh_id hh_id
    happy_hour_month month
    happy_hour_num_of_shaff_in_charge num_of_shaff_in_charge
    happy_hour_member_hh_id hh_id
    happy_hour_member_total_amount total_amount
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
