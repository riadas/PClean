using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("event_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("event_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "event id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[0, "event attendance"], Any[1, "journalist id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[1, "years working"], Any[2, "journalist id"], Any[2, "event id"], Any[2, "work type"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "event id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[0, "event attendance"], Any[1, "journalist id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[1, "years working"], Any[2, "journalist id"], Any[2, "event id"], Any[2, "work type"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model NewsReportModel begin
    @class Event begin
        event_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        name ~ ChooseUniformly(possibilities[:name])
        event_attendance ~ ChooseUniformly(possibilities[:event_attendance])
    end

    @class Journalist begin
        journalist_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        age ~ ChooseUniformly(possibilities[:age])
        years_working ~ ChooseUniformly(possibilities[:years_working])
    end

    @class News_Report begin
        journalist_id ~ Unmodeled()
        event_id ~ ChooseUniformly(possibilities[:event_id])
        work_type ~ ChooseUniformly(possibilities[:work_type])
    end

    @class Obs begin
        event ~ Event
        journalist ~ Journalist
        news_Report ~ News_Report
    end
end

query = @query NewsReportModel.Obs [
    event_id event.event_id
    event_date event.date
    event_venue event.venue
    event_name event.name
    event_attendance event.event_attendance
    journalist_id journalist.journalist_id
    journalist_name journalist.name
    journalist_nationality journalist.nationality
    journalist_age journalist.age
    journalist_years_working journalist.years_working
    news_report_work_type news_Report.work_type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
