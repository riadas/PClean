using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("area_code_state_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("area_code_state_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "area code"], Any[0, "state"], Any[1, "contestant number"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "state"], Any[2, "contestant number"], Any[2, "created"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "area code"], Any[0, "state"], Any[1, "contestant number"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "state"], Any[2, "contestant number"], Any[2, "created"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "area code"], Any[0, "state"], Any[1, "contestant number"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "state"], Any[2, "contestant number"], Any[2, "created"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "area code"], Any[0, "state"], Any[1, "contestant number"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "state"], Any[2, "contestant number"], Any[2, "created"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "area code"], Any[0, "state"], Any[1, "contestant number"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "state"], Any[2, "contestant number"], Any[2, "created"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[8, 3], Any[7, 2]])
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







PClean.@model Voter1Model begin
    @class Area_code_state begin
        area_code ~ ChooseUniformly(possibilities[:area_code])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Contestants begin
        contestant_number ~ ChooseUniformly(possibilities[:contestant_number])
        contestant_name ~ ChooseUniformly(possibilities[:contestant_name])
    end

    @class Votes begin
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        area_code_state ~ Area_code_state
        contestants ~ Contestants
        created ~ TimePrior(possibilities[:created])
    end

    @class Obs begin
        votes ~ Votes
    end
end

query = @query Voter1Model.Obs [
    area_code_state_area_code votes.area_code_state.area_code
    area_code_state_state votes.area_code_state.state
    contestants_contestant_number votes.contestants.contestant_number
    contestants_contestant_name votes.contestants.contestant_name
    votes_vote_id votes.vote_id
    votes_phone_number votes.phone_number
    votes_created votes.created
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
