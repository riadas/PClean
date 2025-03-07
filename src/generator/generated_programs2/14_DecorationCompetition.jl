using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("college_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("college_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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
cols = Any[Any[-1, "*"], Any[0, "college id"], Any[0, "name"], Any[0, "leader name"], Any[0, "college location"], Any[1, "member id"], Any[1, "name"], Any[1, "country"], Any[1, "college id"], Any[2, "round id"], Any[2, "member id"], Any[2, "decoration theme"], Any[2, "rank in round"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[8, 1], Any[10, 5]])
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







PClean.@model DecorationCompetitionModel begin
    @class College begin
        name ~ ChooseUniformly(possibilities[:name])
        leader_name ~ ChooseUniformly(possibilities[:leader_name])
        college_location ~ ChooseUniformly(possibilities[:college_location])
    end

    @class Member begin
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        college ~ College
    end

    @class Round begin
        round_id ~ Unmodeled()
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
