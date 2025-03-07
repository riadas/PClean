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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "college name"], Any[0, "state"], Any[0, "enrollment"], Any[1, "player id"], Any[1, "player name"], Any[1, "yes card"], Any[1, "training hours"], Any[2, "player id"], Any[2, "college name"], Any[2, "player position"], Any[2, "decision"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "college name"], Any[0, "state"], Any[0, "enrollment"], Any[1, "player id"], Any[1, "player name"], Any[1, "yes card"], Any[1, "training hours"], Any[2, "player id"], Any[2, "college name"], Any[2, "player position"], Any[2, "decision"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "college name"], Any[0, "state"], Any[0, "enrollment"], Any[1, "player id"], Any[1, "player name"], Any[1, "yes card"], Any[1, "training hours"], Any[2, "player id"], Any[2, "college name"], Any[2, "player position"], Any[2, "decision"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "college name"], Any[0, "state"], Any[0, "enrollment"], Any[1, "player id"], Any[1, "player name"], Any[1, "yes card"], Any[1, "training hours"], Any[2, "player id"], Any[2, "college name"], Any[2, "player position"], Any[2, "decision"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "college name"], Any[0, "state"], Any[0, "enrollment"], Any[1, "player id"], Any[1, "player name"], Any[1, "yes card"], Any[1, "training hours"], Any[2, "player id"], Any[2, "college name"], Any[2, "player position"], Any[2, "decision"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 1], Any[8, 4]])
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







PClean.@model Soccer2Model begin
    @class College begin
        college_name ~ ChooseUniformly(possibilities[:college_name])
        state ~ ChooseUniformly(possibilities[:state])
        enrollment ~ ChooseUniformly(possibilities[:enrollment])
    end

    @class Player begin
        player_name ~ ChooseUniformly(possibilities[:player_name])
        yes_card ~ ChooseUniformly(possibilities[:yes_card])
        training_hours ~ ChooseUniformly(possibilities[:training_hours])
    end

    @class Tryout begin
        college ~ College
        player_position ~ ChooseUniformly(possibilities[:player_position])
        decision ~ ChooseUniformly(possibilities[:decision])
    end

    @class Obs begin
        tryout ~ Tryout
    end
end

query = @query Soccer2Model.Obs [
    college_name tryout.college.college_name
    college_state tryout.college.state
    college_enrollment tryout.college.enrollment
    player_id tryout.player.player_id
    player_name tryout.player.player_name
    player_yes_card tryout.player.yes_card
    player_training_hours tryout.player.training_hours
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
