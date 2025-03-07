using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("people_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("people_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "people id"], Any[0, "name"], Any[0, "country"], Any[0, "is male"], Any[0, "age"], Any[1, "church id"], Any[1, "name"], Any[1, "organized by"], Any[1, "open date"], Any[1, "continuation of"], Any[2, "church id"], Any[2, "male id"], Any[2, "female id"], Any[2, "year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 1], Any[12, 1], Any[11, 6]])
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







PClean.@model WeddingModel begin
    @class People begin
        name ~ ChooseUniformly(possibilities[:name])
        country ~ ChooseUniformly(possibilities[:country])
        is_male ~ ChooseUniformly(possibilities[:is_male])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Church begin
        name ~ ChooseUniformly(possibilities[:name])
        organized_by ~ ChooseUniformly(possibilities[:organized_by])
        open_date ~ ChooseUniformly(possibilities[:open_date])
        continuation_of ~ ChooseUniformly(possibilities[:continuation_of])
    end

    @class Wedding begin
        people ~ People
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        wedding ~ Wedding
    end
end

query = @query WeddingModel.Obs [
    people_id wedding.people.people_id
    people_name wedding.people.name
    people_country wedding.people.country
    people_is_male wedding.people.is_male
    people_age wedding.people.age
    church_id wedding.church.church_id
    church_name wedding.church.name
    church_organized_by wedding.church.organized_by
    church_open_date wedding.church.open_date
    church_continuation_of wedding.church.continuation_of
    wedding_year wedding.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
