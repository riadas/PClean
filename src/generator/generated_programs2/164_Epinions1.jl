using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("item_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("item_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "item id"], Any[0, "title"], Any[1, "a id"], Any[1, "user id"], Any[1, "item id"], Any[1, "rating"], Any[1, "rank"], Any[2, "user id"], Any[2, "name"], Any[3, "source user id"], Any[3, "target user id"], Any[3, "trust"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 1], Any[4, 8], Any[11, 8], Any[10, 8]])
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







PClean.@model Epinions1Model begin
    @class Item begin
        title ~ ChooseUniformly(possibilities[:title])
    end

    @class Useracct begin
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Trust begin
        useracct ~ Useracct
        trust ~ ChooseUniformly(possibilities[:trust])
    end

    @class Review begin
        useracct ~ Useracct
        item ~ Item
        rating ~ ChooseUniformly(possibilities[:rating])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Obs begin
        review ~ Review
        trust ~ Trust
    end
end

query = @query Epinions1Model.Obs [
    item_id review.item.item_id
    item_title review.item.title
    review_a_id review.a_id
    review_rating review.rating
    review_rank review.rank
    useracct_user_id review.useracct.user_id
    useracct_name review.useracct.name
    trust trust.trust
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
