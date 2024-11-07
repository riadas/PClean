using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("conductor_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("conductor_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "conductor id"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra id"], Any[1, "orchestra"], Any[1, "conductor id"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "performance id"], Any[2, "orchestra id"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "performance id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "conductor id"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra id"], Any[1, "orchestra"], Any[1, "conductor id"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "performance id"], Any[2, "orchestra id"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "performance id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model OrchestraModel begin
    @class Conductor begin
        conductor_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        year_of_work ~ ChooseUniformly(possibilities[:year_of_work])
    end

    @class Orchestra begin
        orchestra_id ~ Unmodeled()
        orchestra ~ ChooseUniformly(possibilities[:orchestra])
        conductor_id ~ ChooseUniformly(possibilities[:conductor_id])
        record_company ~ ChooseUniformly(possibilities[:record_company])
        year_of_founded ~ ChooseUniformly(possibilities[:year_of_founded])
        major_record_format ~ ChooseUniformly(possibilities[:major_record_format])
    end

    @class Performance begin
        performance_id ~ Unmodeled()
        orchestra_id ~ ChooseUniformly(possibilities[:orchestra_id])
        type ~ ChooseUniformly(possibilities[:type])
        date ~ ChooseUniformly(possibilities[:date])
        official_ratings_(millions) ~ ChooseUniformly(possibilities[:official_ratings_(millions)])
        weekly_rank ~ ChooseUniformly(possibilities[:weekly_rank])
        share ~ ChooseUniformly(possibilities[:share])
    end

    @class Show begin
        show_id ~ Unmodeled()
        performance_id ~ ChooseUniformly(possibilities[:performance_id])
        if_first_show ~ ChooseUniformly(possibilities[:if_first_show])
        result ~ ChooseUniformly(possibilities[:result])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end

    @class Obs begin
        conductor ~ Conductor
        orchestra ~ Orchestra
        performance ~ Performance
        show ~ Show
    end
end

query = @query OrchestraModel.Obs [
    conductor_id conductor.conductor_id
    conductor_name conductor.name
    conductor_age conductor.age
    conductor_nationality conductor.nationality
    conductor_year_of_work conductor.year_of_work
    orchestra_id orchestra.orchestra_id
    orchestra orchestra.orchestra
    orchestra_record_company orchestra.record_company
    orchestra_year_of_founded orchestra.year_of_founded
    orchestra_major_record_format orchestra.major_record_format
    performance_id performance.performance_id
    performance_type performance.type
    performance_date performance.date
    performance_official_ratings_(millions) performance.official_ratings_(millions)
    performance_weekly_rank performance.weekly_rank
    performance_share performance.share
    show_id show.show_id
    if_first_show show.if_first_show
    show_result show.result
    show_attendance show.attendance
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
