using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("item_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("item_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "item id"], Any[0, "title"], Any[1, "a id"], Any[1, "user id"], Any[1, "item id"], Any[1, "rating"], Any[1, "rank"], Any[2, "user id"], Any[2, "name"], Any[3, "source user id"], Any[3, "target user id"], Any[3, "trust"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "item id"], Any[0, "title"], Any[1, "a id"], Any[1, "user id"], Any[1, "item id"], Any[1, "rating"], Any[1, "rank"], Any[2, "user id"], Any[2, "name"], Any[3, "source user id"], Any[3, "target user id"], Any[3, "trust"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["item id", "user id", "target user id", "source user id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "title"], Any[1, "a id"], Any[1, "rating"], Any[1, "rank"], Any[2, "name"], Any[3, "trust"]]
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





PClean.@model Epinions1Model begin
    @class Item begin
        item_id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
    end

    @class Useracct begin
        user_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Obs begin
        item ~ Item
        useracct ~ Useracct
        a_id ~ Unmodeled()
        rating ~ ChooseUniformly(possibilities[:rating])
        rank ~ ChooseUniformly(possibilities[:rank])
        trust ~ ChooseUniformly(possibilities[:trust])
    end
end

query = @query Epinions1Model.Obs [
    item_id item.item_id
    item_title item.title
    review_a_id a_id
    review_rating rating
    review_rank rank
    useracct_user_id useracct.user_id
    useracct_name useracct.name
    trust trust
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
