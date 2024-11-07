using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("college_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("college_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "college id"], Any[0, "name"], Any[0, "leader name"], Any[0, "college location"], Any[1, "member id"], Any[1, "name"], Any[1, "country"], Any[1, "college id"], Any[2, "round id"], Any[2, "member id"], Any[2, "decoration theme"], Any[2, "rank in round"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "college id"], Any[0, "name"], Any[0, "leader name"], Any[0, "college location"], Any[1, "member id"], Any[1, "name"], Any[1, "country"], Any[1, "college id"], Any[2, "round id"], Any[2, "member id"], Any[2, "decoration theme"], Any[2, "rank in round"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model DecorationCompetitionModel begin
    @class College begin
        college_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        leader_name ~ ChooseUniformly(possibilities[:leader_name])
        college_location ~ ChooseUniformly(possibilities[:college_location])
    end

    @class Member begin
        member_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        college_id ~ ChooseUniformly(possibilities[:college_id])
    end

    @class Round begin
        round_id ~ Unmodeled()
        member_id ~ ChooseUniformly(possibilities[:member_id])
        decoration_theme ~ ChooseUniformly(possibilities[:decoration_theme])
        rank_in_round ~ ChooseUniformly(possibilities[:rank_in_round])
    end

    @class Obs begin
        college ~ College
        member ~ Member
        round ~ Round
    end
end

query = @query DecorationCompetitionModel.Obs [
    college_id college.college_id
    college_name college.name
    college_leader_name college.leader_name
    college_location college.college_location
    member_id member.member_id
    member_name member.name
    member_country member.country
    round_id round.round_id
    round_decoration_theme round.decoration_theme
    rank_in_round round.rank_in_round
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
