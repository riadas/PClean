using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("mountain_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("mountain_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "mountain id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "climber id"], Any[1, "name"], Any[1, "country"], Any[1, "time"], Any[1, "points"], Any[1, "mountain id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "mountain id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "climber id"], Any[1, "name"], Any[1, "country"], Any[1, "time"], Any[1, "points"], Any[1, "mountain id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ClimbingModel begin
    @class Mountain begin
        mountain_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        prominence ~ ChooseUniformly(possibilities[:prominence])
        range ~ ChooseUniformly(possibilities[:range])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Climber begin
        climber_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        time ~ ChooseUniformly(possibilities[:time])
        points ~ ChooseUniformly(possibilities[:points])
        mountain_id ~ ChooseUniformly(possibilities[:mountain_id])
    end

    @class Obs begin
        mountain ~ Mountain
        climber ~ Climber
    end
end

query = @query ClimbingModel.Obs [
    mountain_id mountain.mountain_id
    mountain_name mountain.name
    mountain_height mountain.height
    mountain_prominence mountain.prominence
    mountain_range mountain.range
    mountain_country mountain.country
    climber_id climber.climber_id
    climber_name climber.name
    climber_country climber.country
    climber_time climber.time
    climber_points climber.points
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
