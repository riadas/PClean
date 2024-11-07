using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("area code state_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("area code state_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "area code"], Any[0, "state"], Any[1, "contestant number"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "state"], Any[2, "contestant number"], Any[2, "created"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "area code"], Any[0, "state"], Any[1, "contestant number"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "state"], Any[2, "contestant number"], Any[2, "created"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Voter1Model begin
    @class Area_Code_State begin
        area_code ~ ChooseUniformly(possibilities[:area_code])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Contestants begin
        contestant_number ~ ChooseUniformly(possibilities[:contestant_number])
        contestant_name ~ ChooseUniformly(possibilities[:contestant_name])
    end

    @class Votes begin
        vote_id ~ Unmodeled()
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        state ~ ChooseUniformly(possibilities[:state])
        contestant_number ~ ChooseUniformly(possibilities[:contestant_number])
        created ~ TimePrior(possibilities[:created])
    end

    @class Obs begin
        area_Code_State ~ Area_Code_State
        contestants ~ Contestants
        votes ~ Votes
    end
end

query = @query Voter1Model.Obs [
    area_code_state_area_code area_Code_State.area_code
    area_code_state_state area_Code_State.state
    contestants_contestant_number contestants.contestant_number
    contestants_contestant_name contestants.contestant_name
    votes_vote_id votes.vote_id
    votes_phone_number votes.phone_number
    votes_created votes.created
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
