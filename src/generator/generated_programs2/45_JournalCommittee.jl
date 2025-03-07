using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("journal_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("journal_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "journal id"], Any[0, "date"], Any[0, "theme"], Any[0, "sales"], Any[1, "editor id"], Any[1, "name"], Any[1, "age"], Any[2, "editor id"], Any[2, "journal id"], Any[2, "work type"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[9, 1], Any[8, 5]])
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







PClean.@model JournalCommitteeModel begin
    @class Journal begin
        date ~ ChooseUniformly(possibilities[:date])
        theme ~ ChooseUniformly(possibilities[:theme])
        sales ~ ChooseUniformly(possibilities[:sales])
    end

    @class Editor begin
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Journal_committee begin
        journal ~ Journal
        work_type ~ ChooseUniformly(possibilities[:work_type])
    end

    @class Obs begin
        journal_committee ~ Journal_committee
    end
end

query = @query JournalCommitteeModel.Obs [
    journal_id journal_committee.journal.journal_id
    journal_date journal_committee.journal.date
    journal_theme journal_committee.journal.theme
    journal_sales journal_committee.journal.sales
    editor_id journal_committee.editor.editor_id
    editor_name journal_committee.editor.name
    editor_age journal_committee.editor.age
    journal_committee_work_type journal_committee.work_type
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
