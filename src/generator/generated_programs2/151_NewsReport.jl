using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("event_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("event_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "event id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[0, "event attendance"], Any[1, "journalist id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[1, "years working"], Any[2, "journalist id"], Any[2, "event id"], Any[2, "work type"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "event id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[0, "event attendance"], Any[1, "journalist id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[1, "years working"], Any[2, "journalist id"], Any[2, "event id"], Any[2, "work type"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "event id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[0, "event attendance"], Any[1, "journalist id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[1, "years working"], Any[2, "journalist id"], Any[2, "event id"], Any[2, "work type"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "event id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[0, "event attendance"], Any[1, "journalist id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[1, "years working"], Any[2, "journalist id"], Any[2, "event id"], Any[2, "work type"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "event id"], Any[0, "date"], Any[0, "venue"], Any[0, "name"], Any[0, "event attendance"], Any[1, "journalist id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[1, "years working"], Any[2, "journalist id"], Any[2, "event id"], Any[2, "work type"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 1], Any[11, 6]])
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







PClean.@model NewsReportModel begin
    @class Event begin
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        name ~ ChooseUniformly(possibilities[:name])
        event_attendance ~ ChooseUniformly(possibilities[:event_attendance])
    end

    @class Journalist begin
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        age ~ ChooseUniformly(possibilities[:age])
        years_working ~ ChooseUniformly(possibilities[:years_working])
    end

    @class News_report begin
        event ~ Event
        work_type ~ ChooseUniformly(possibilities[:work_type])
    end

    @class Obs begin
        news_report ~ News_report
    end
end

query = @query NewsReportModel.Obs [
    event_id news_report.event.event_id
    event_date news_report.event.date
    event_venue news_report.event.venue
    event_name news_report.event.name
    event_attendance news_report.event.event_attendance
    journalist_id news_report.journalist.journalist_id
    journalist_name news_report.journalist.name
    journalist_nationality news_report.journalist.nationality
    journalist_age news_report.journalist.age
    journalist_years_working news_report.journalist.years_working
    news_report_work_type news_report.work_type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
