using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("member_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("member_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "member id"], Any[0, "card number"], Any[0, "name"], Any[0, "hometown"], Any[0, "level"], Any[1, "branch id"], Any[1, "name"], Any[1, "open year"], Any[1, "address road"], Any[1, "city"], Any[1, "membership amount"], Any[2, "member id"], Any[2, "branch id"], Any[2, "register year"], Any[3, "member id"], Any[3, "branch id"], Any[3, "year"], Any[3, "total pounds"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "member id"], Any[0, "card number"], Any[0, "name"], Any[0, "hometown"], Any[0, "level"], Any[1, "branch id"], Any[1, "name"], Any[1, "open year"], Any[1, "address road"], Any[1, "city"], Any[1, "membership amount"], Any[2, "member id"], Any[2, "branch id"], Any[2, "register year"], Any[3, "member id"], Any[3, "branch id"], Any[3, "year"], Any[3, "total pounds"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "member id"], Any[0, "card number"], Any[0, "name"], Any[0, "hometown"], Any[0, "level"], Any[1, "branch id"], Any[1, "name"], Any[1, "open year"], Any[1, "address road"], Any[1, "city"], Any[1, "membership amount"], Any[2, "member id"], Any[2, "branch id"], Any[2, "register year"], Any[3, "member id"], Any[3, "branch id"], Any[3, "year"], Any[3, "total pounds"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "member id"], Any[0, "card number"], Any[0, "name"], Any[0, "hometown"], Any[0, "level"], Any[1, "branch id"], Any[1, "name"], Any[1, "open year"], Any[1, "address road"], Any[1, "city"], Any[1, "membership amount"], Any[2, "member id"], Any[2, "branch id"], Any[2, "register year"], Any[3, "member id"], Any[3, "branch id"], Any[3, "year"], Any[3, "total pounds"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "member id"], Any[0, "card number"], Any[0, "name"], Any[0, "hometown"], Any[0, "level"], Any[1, "branch id"], Any[1, "name"], Any[1, "open year"], Any[1, "address road"], Any[1, "city"], Any[1, "membership amount"], Any[2, "member id"], Any[2, "branch id"], Any[2, "register year"], Any[3, "member id"], Any[3, "branch id"], Any[3, "year"], Any[3, "total pounds"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 6], Any[12, 1], Any[16, 6], Any[15, 1]])
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







PClean.@model ShopMembershipModel begin
    @class Member begin
        card_number ~ ChooseUniformly(possibilities[:card_number])
        name ~ ChooseUniformly(possibilities[:name])
        hometown ~ ChooseUniformly(possibilities[:hometown])
        level ~ ChooseUniformly(possibilities[:level])
    end

    @class Branch begin
        name ~ ChooseUniformly(possibilities[:name])
        open_year ~ ChooseUniformly(possibilities[:open_year])
        address_road ~ ChooseUniformly(possibilities[:address_road])
        city ~ ChooseUniformly(possibilities[:city])
        membership_amount ~ ChooseUniformly(possibilities[:membership_amount])
    end

    @class Membership_register_branch begin
        branch ~ Branch
        register_year ~ ChooseUniformly(possibilities[:register_year])
    end

    @class Purchase begin
        branch ~ Branch
        year ~ ChooseUniformly(possibilities[:year])
        total_pounds ~ ChooseUniformly(possibilities[:total_pounds])
    end

    @class Obs begin
        membership_register_branch ~ Membership_register_branch
        purchase ~ Purchase
    end
end

query = @query ShopMembershipModel.Obs [
    member_id membership_register_branch.member.member_id
    member_card_number membership_register_branch.member.card_number
    member_name membership_register_branch.member.name
    member_hometown membership_register_branch.member.hometown
    member_level membership_register_branch.member.level
    branch_id membership_register_branch.branch.branch_id
    branch_name membership_register_branch.branch.name
    branch_open_year membership_register_branch.branch.open_year
    branch_address_road membership_register_branch.branch.address_road
    branch_city membership_register_branch.branch.city
    branch_membership_amount membership_register_branch.branch.membership_amount
    membership_register_branch_register_year membership_register_branch.register_year
    purchase_year purchase.year
    purchase_total_pounds purchase.total_pounds
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
