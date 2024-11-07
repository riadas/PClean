using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("item_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("item_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "item id"], Any[0, "title"], Any[1, "a id"], Any[1, "user id"], Any[1, "item id"], Any[1, "rating"], Any[1, "rank"], Any[2, "user id"], Any[2, "name"], Any[3, "source user id"], Any[3, "target user id"], Any[3, "trust"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "item id"], Any[0, "title"], Any[1, "a id"], Any[1, "user id"], Any[1, "item id"], Any[1, "rating"], Any[1, "rank"], Any[2, "user id"], Any[2, "name"], Any[3, "source user id"], Any[3, "target user id"], Any[3, "trust"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Review begin
        a_id ~ Unmodeled()
        user_id ~ ChooseUniformly(possibilities[:user_id])
        item_id ~ ChooseUniformly(possibilities[:item_id])
        rating ~ ChooseUniformly(possibilities[:rating])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Useracct begin
        user_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Trust begin
        source_user_id ~ Unmodeled()
        target_user_id ~ ChooseUniformly(possibilities[:target_user_id])
        trust ~ ChooseUniformly(possibilities[:trust])
    end

    @class Obs begin
        item ~ Item
        review ~ Review
        useracct ~ Useracct
        trust ~ Trust
    end
end

query = @query Epinions1Model.Obs [
    item_id item.item_id
    item_title item.title
    review_a_id review.a_id
    review_rating review.rating
    review_rank review.rank
    useracct_user_id useracct.user_id
    useracct_name useracct.name
    trust trust.trust
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
