using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("people_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("people_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "people id"], Any[0, "name"], Any[0, "country"], Any[0, "is male"], Any[0, "age"], Any[1, "church id"], Any[1, "name"], Any[1, "organized by"], Any[1, "open date"], Any[1, "continuation of"], Any[2, "church id"], Any[2, "male id"], Any[2, "female id"], Any[2, "year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "people id"], Any[0, "name"], Any[0, "country"], Any[0, "is male"], Any[0, "age"], Any[1, "church id"], Any[1, "name"], Any[1, "organized by"], Any[1, "open date"], Any[1, "continuation of"], Any[2, "church id"], Any[2, "male id"], Any[2, "female id"], Any[2, "year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["female id", "male id", "church id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "people id"], Any[0, "name"], Any[0, "country"], Any[0, "is male"], Any[0, "age"], Any[1, "name"], Any[1, "organized by"], Any[1, "open date"], Any[1, "continuation of"], Any[2, "year"]]
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





PClean.@model WeddingModel begin
    @class People begin
        people_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        is_male ~ ChooseUniformly(possibilities[:is_male])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Church begin
        church_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        organized_by ~ ChooseUniformly(possibilities[:organized_by])
        open_date ~ ChooseUniformly(possibilities[:open_date])
        continuation_of ~ ChooseUniformly(possibilities[:continuation_of])
    end

    @class Obs begin
        people ~ People
        church ~ Church
        year ~ ChooseUniformly(possibilities[:year])
    end
end

query = @query WeddingModel.Obs [
    people_id people.people_id
    people_name people.name
    people_country people.country
    people_is_male people.is_male
    people_age people.age
    church_id church.church_id
    church_name church.name
    church_organized_by church.organized_by
    church_open_date church.open_date
    church_continuation_of church.continuation_of
    wedding_year year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
