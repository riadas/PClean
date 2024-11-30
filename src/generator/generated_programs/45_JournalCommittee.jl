using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("journal_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("journal_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "journal id"], Any[0, "date"], Any[0, "theme"], Any[0, "sales"], Any[1, "editor id"], Any[1, "name"], Any[1, "age"], Any[2, "editor id"], Any[2, "journal id"], Any[2, "work type"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "journal id"], Any[0, "date"], Any[0, "theme"], Any[0, "sales"], Any[1, "editor id"], Any[1, "name"], Any[1, "age"], Any[2, "editor id"], Any[2, "journal id"], Any[2, "work type"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["journal id", "editor id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "date"], Any[0, "theme"], Any[0, "sales"], Any[1, "name"], Any[1, "age"], Any[2, "work type"]]
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





PClean.@model JournalCommitteeModel begin
    @class Journal begin
        journal_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        theme ~ ChooseUniformly(possibilities[:theme])
        sales ~ ChooseUniformly(possibilities[:sales])
    end

    @class Editor begin
        editor_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Obs begin
        journal ~ Journal
        editor ~ Editor
        work_type ~ ChooseUniformly(possibilities[:work_type])
    end
end

query = @query JournalCommitteeModel.Obs [
    journal_id journal.journal_id
    journal_date journal.date
    journal_theme journal.theme
    journal_sales journal.sales
    editor_id editor.editor_id
    editor_name editor.name
    editor_age editor.age
    journal_committee_work_type work_type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
