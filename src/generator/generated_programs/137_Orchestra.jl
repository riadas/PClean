using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("conductor_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("conductor_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["conductor id", "orchestra id", "performance id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "age"], Any[0, "nationality"], Any[0, "year of work"], Any[1, "orchestra"], Any[1, "record company"], Any[1, "year of founded"], Any[1, "major record format"], Any[2, "type"], Any[2, "date"], Any[2, "official ratings (millions)"], Any[2, "weekly rank"], Any[2, "share"], Any[3, "show id"], Any[3, "if first show"], Any[3, "result"], Any[3, "attendance"]]
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





PClean.@model OrchestraModel begin
    @class Conductor begin
        conductor_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        year_of_work ~ ChooseUniformly(possibilities[:year_of_work])
    end

    @class Obs begin
        conductor ~ Conductor
        orchestra_id ~ Unmodeled()
        orchestra ~ ChooseUniformly(possibilities[:orchestra])
        record_company ~ ChooseUniformly(possibilities[:record_company])
        year_of_founded ~ ChooseUniformly(possibilities[:year_of_founded])
        major_record_format ~ ChooseUniformly(possibilities[:major_record_format])
        performance_id ~ Unmodeled()
        type ~ ChooseUniformly(possibilities[:type])
        date ~ ChooseUniformly(possibilities[:date])
        official_ratings_(millions) ~ ChooseUniformly(possibilities[:official_ratings_(millions)])
        weekly_rank ~ ChooseUniformly(possibilities[:weekly_rank])
        share ~ ChooseUniformly(possibilities[:share])
        show_id ~ Unmodeled()
        if_first_show ~ ChooseUniformly(possibilities[:if_first_show])
        result ~ ChooseUniformly(possibilities[:result])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end
end

query = @query OrchestraModel.Obs [
    conductor_id conductor.conductor_id
    conductor_name conductor.name
    conductor_age conductor.age
    conductor_nationality conductor.nationality
    conductor_year_of_work conductor.year_of_work
    orchestra_id orchestra_id
    orchestra orchestra
    orchestra_record_company record_company
    orchestra_year_of_founded year_of_founded
    orchestra_major_record_format major_record_format
    performance_id performance_id
    performance_type type
    performance_date date
    performance_official_ratings_(millions) official_ratings_(millions)
    performance_weekly_rank weekly_rank
    performance_share share
    show_id show_id
    if_first_show if_first_show
    show_result result
    show_attendance attendance
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
