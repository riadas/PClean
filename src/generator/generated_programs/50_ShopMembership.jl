using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("member_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("member_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "member id"], Any[0, "card number"], Any[0, "name"], Any[0, "hometown"], Any[0, "level"], Any[1, "branch id"], Any[1, "name"], Any[1, "open year"], Any[1, "address road"], Any[1, "city"], Any[1, "membership amount"], Any[2, "member id"], Any[2, "branch id"], Any[2, "register year"], Any[3, "member id"], Any[3, "branch id"], Any[3, "year"], Any[3, "total pounds"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "member id"], Any[0, "card number"], Any[0, "name"], Any[0, "hometown"], Any[0, "level"], Any[1, "branch id"], Any[1, "name"], Any[1, "open year"], Any[1, "address road"], Any[1, "city"], Any[1, "membership amount"], Any[2, "member id"], Any[2, "branch id"], Any[2, "register year"], Any[3, "member id"], Any[3, "branch id"], Any[3, "year"], Any[3, "total pounds"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ShopMembershipModel begin
    @class Member begin
        member_id ~ Unmodeled()
        card_number ~ ChooseUniformly(possibilities[:card_number])
        name ~ ChooseUniformly(possibilities[:name])
        hometown ~ ChooseUniformly(possibilities[:hometown])
        level ~ ChooseUniformly(possibilities[:level])
    end

    @class Branch begin
        branch_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        open_year ~ ChooseUniformly(possibilities[:open_year])
        address_road ~ ChooseUniformly(possibilities[:address_road])
        city ~ ChooseUniformly(possibilities[:city])
        membership_amount ~ ChooseUniformly(possibilities[:membership_amount])
    end

    @class Membership_Register_Branch begin
        member_id ~ Unmodeled()
        branch_id ~ ChooseUniformly(possibilities[:branch_id])
        register_year ~ ChooseUniformly(possibilities[:register_year])
    end

    @class Purchase begin
        member_id ~ Unmodeled()
        branch_id ~ ChooseUniformly(possibilities[:branch_id])
        year ~ ChooseUniformly(possibilities[:year])
        total_pounds ~ ChooseUniformly(possibilities[:total_pounds])
    end

    @class Obs begin
        member ~ Member
        branch ~ Branch
        membership_Register_Branch ~ Membership_Register_Branch
        purchase ~ Purchase
    end
end

query = @query ShopMembershipModel.Obs [
    member_id member.member_id
    member_card_number member.card_number
    member_name member.name
    member_hometown member.hometown
    member_level member.level
    branch_id branch.branch_id
    branch_name branch.name
    branch_open_year branch.open_year
    branch_address_road branch.address_road
    branch_city branch.city
    branch_membership_amount branch.membership_amount
    membership_register_branch_register_year membership_Register_Branch.register_year
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
