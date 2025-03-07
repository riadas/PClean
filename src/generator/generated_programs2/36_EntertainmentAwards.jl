using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("festival_detail_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("festival_detail_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "festival id"], Any[0, "festival name"], Any[0, "chair name"], Any[0, "location"], Any[0, "year"], Any[0, "num of audience"], Any[1, "artwork id"], Any[1, "type"], Any[1, "name"], Any[2, "artwork id"], Any[2, "festival id"], Any[2, "result"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "festival id"], Any[0, "festival name"], Any[0, "chair name"], Any[0, "location"], Any[0, "year"], Any[0, "num of audience"], Any[1, "artwork id"], Any[1, "type"], Any[1, "name"], Any[2, "artwork id"], Any[2, "festival id"], Any[2, "result"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "festival id"], Any[0, "festival name"], Any[0, "chair name"], Any[0, "location"], Any[0, "year"], Any[0, "num of audience"], Any[1, "artwork id"], Any[1, "type"], Any[1, "name"], Any[2, "artwork id"], Any[2, "festival id"], Any[2, "result"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "festival id"], Any[0, "festival name"], Any[0, "chair name"], Any[0, "location"], Any[0, "year"], Any[0, "num of audience"], Any[1, "artwork id"], Any[1, "type"], Any[1, "name"], Any[2, "artwork id"], Any[2, "festival id"], Any[2, "result"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "festival id"], Any[0, "festival name"], Any[0, "chair name"], Any[0, "location"], Any[0, "year"], Any[0, "num of audience"], Any[1, "artwork id"], Any[1, "type"], Any[1, "name"], Any[2, "artwork id"], Any[2, "festival id"], Any[2, "result"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 1], Any[10, 7]])
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







PClean.@model EntertainmentAwardsModel begin
    @class Festival_detail begin
        festival_name ~ ChooseUniformly(possibilities[:festival_name])
        chair_name ~ ChooseUniformly(possibilities[:chair_name])
        location ~ ChooseUniformly(possibilities[:location])
        year ~ ChooseUniformly(possibilities[:year])
        num_of_audience ~ ChooseUniformly(possibilities[:num_of_audience])
    end

    @class Artwork begin
        type ~ ChooseUniformly(possibilities[:type])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Nomination begin
        festival_detail ~ Festival_detail
        result ~ ChooseUniformly(possibilities[:result])
    end

    @class Obs begin
        nomination ~ Nomination
    end
end

query = @query EntertainmentAwardsModel.Obs [
    festival_detail_festival_id nomination.festival_detail.festival_id
    festival_detail_festival_name nomination.festival_detail.festival_name
    festival_detail_chair_name nomination.festival_detail.chair_name
    festival_detail_location nomination.festival_detail.location
    festival_detail_year nomination.festival_detail.year
    festival_detail_num_of_audience nomination.festival_detail.num_of_audience
    artwork_id nomination.artwork.artwork_id
    artwork_type nomination.artwork.type
    artwork_name nomination.artwork.name
    nomination_result nomination.result
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
