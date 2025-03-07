using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("conductor_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("conductor_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "conductor id"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra id"], Any[1, "orchestra"], Any[1, "conductor id"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "performance id"], Any[2, "orchestra id"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "performance id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "conductor id"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra id"], Any[1, "orchestra"], Any[1, "conductor id"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "performance id"], Any[2, "orchestra id"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "performance id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "conductor id"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra id"], Any[1, "orchestra"], Any[1, "conductor id"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "performance id"], Any[2, "orchestra id"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "performance id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "conductor id"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra id"], Any[1, "orchestra"], Any[1, "conductor id"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "performance id"], Any[2, "orchestra id"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "performance id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "conductor id"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra id"], Any[1, "orchestra"], Any[1, "conductor id"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "performance id"], Any[2, "orchestra id"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "performance id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[8, 1], Any[13, 6], Any[20, 12]])
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







PClean.@model OrchestraModel begin
    @class Conductor begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        year_of_work ~ ChooseUniformly(possibilities[:year_of_work])
    end

    @class Orchestra begin
        orchestra ~ ChooseUniformly(possibilities[:orchestra])
        conductor ~ Conductor
        record_company ~ ChooseUniformly(possibilities[:record_company])
        year_of_founded ~ ChooseUniformly(possibilities[:year_of_founded])
        major_record_format ~ ChooseUniformly(possibilities[:major_record_format])
    end

    @class Performance begin
        orchestra ~ Orchestra
        type ~ ChooseUniformly(possibilities[:type])
        date ~ ChooseUniformly(possibilities[:date])
        official_ratings_(millions) ~ ChooseUniformly(possibilities[:official_ratings_(millions)])
        weekly_rank ~ ChooseUniformly(possibilities[:weekly_rank])
        share ~ ChooseUniformly(possibilities[:share])
    end

    @class Show begin
        show_id ~ Unmodeled()
        performance ~ Performance
        if_first_show ~ ChooseUniformly(possibilities[:if_first_show])
        result ~ ChooseUniformly(possibilities[:result])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end

    @class Obs begin
        show ~ Show
    end
end

query = @query OrchestraModel.Obs [
    conductor_id show.performance.orchestra.conductor.conductor_id
    conductor_name show.performance.orchestra.conductor.name
    conductor_age show.performance.orchestra.conductor.age
    conductor_nationality show.performance.orchestra.conductor.nationality
    conductor_year_of_work show.performance.orchestra.conductor.year_of_work
    orchestra_id show.performance.orchestra.orchestra_id
    orchestra show.performance.orchestra.orchestra
    orchestra_record_company show.performance.orchestra.record_company
    orchestra_year_of_founded show.performance.orchestra.year_of_founded
    orchestra_major_record_format show.performance.orchestra.major_record_format
    performance_id show.performance.performance_id
    performance_type show.performance.type
    performance_date show.performance.date
    performance_official_ratings_(millions) show.performance.official_ratings_(millions)
    performance_weekly_rank show.performance.weekly_rank
    performance_share show.performance.share
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
