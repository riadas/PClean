using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("college_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("college_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "college name"], Any[0, "state"], Any[0, "enrollment"], Any[1, "player id"], Any[1, "player name"], Any[1, "yes card"], Any[1, "training hours"], Any[2, "player id"], Any[2, "college name"], Any[2, "player position"], Any[2, "decision"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "college name"], Any[0, "state"], Any[0, "enrollment"], Any[1, "player id"], Any[1, "player name"], Any[1, "yes card"], Any[1, "training hours"], Any[2, "player id"], Any[2, "college name"], Any[2, "player position"], Any[2, "decision"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Soccer2Model begin
    @class College begin
        college_name ~ ChooseUniformly(possibilities[:college_name])
        state ~ ChooseUniformly(possibilities[:state])
        enrollment ~ ChooseUniformly(possibilities[:enrollment])
    end

    @class Player begin
        player_id ~ Unmodeled()
        player_name ~ ChooseUniformly(possibilities[:player_name])
        yes_card ~ ChooseUniformly(possibilities[:yes_card])
        training_hours ~ ChooseUniformly(possibilities[:training_hours])
    end

    @class Tryout begin
        player_id ~ Unmodeled()
        college_name ~ ChooseUniformly(possibilities[:college_name])
        player_position ~ ChooseUniformly(possibilities[:player_position])
        decision ~ ChooseUniformly(possibilities[:decision])
    end

    @class Obs begin
        college ~ College
        player ~ Player
        tryout ~ Tryout
    end
end

query = @query Soccer2Model.Obs [
    college_name college.college_name
    college_state college.state
    college_enrollment college.enrollment
    player_id player.player_id
    player_name player.player_name
    player_yes_card player.yes_card
    player_training_hours player.training_hours
    tryout_player_position tryout.player_position
    tryout_decision tryout.decision
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
