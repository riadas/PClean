using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("college_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("college_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "college id"], Any[0, "name"], Any[0, "leader name"], Any[0, "college location"], Any[1, "member id"], Any[1, "name"], Any[1, "country"], Any[1, "college id"], Any[2, "round id"], Any[2, "member id"], Any[2, "decoration theme"], Any[2, "rank in round"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "college id"], Any[0, "name"], Any[0, "leader name"], Any[0, "college location"], Any[1, "member id"], Any[1, "name"], Any[1, "country"], Any[1, "college id"], Any[2, "round id"], Any[2, "member id"], Any[2, "decoration theme"], Any[2, "rank in round"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["college id", "member id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "leader name"], Any[0, "college location"], Any[1, "name"], Any[1, "country"], Any[2, "round id"], Any[2, "decoration theme"], Any[2, "rank in round"]]
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
        college ~ College
    end

    @class Round begin
        round_id ~ Unmodeled()
        member ~ Member
        decoration_theme ~ ChooseUniformly(possibilities[:decoration_theme])
        rank_in_round ~ ChooseUniformly(possibilities[:rank_in_round])
    end

    @class Obs begin
        round ~ Round
    end
end

query = @query DecorationCompetitionModel.Obs [
    college_id round.member.college.college_id
    college_name round.member.college.name
    college_leader_name round.member.college.leader_name
    college_location round.member.college.college_location
    member_id round.member.member_id
    member_name round.member.name
    member_country round.member.country
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