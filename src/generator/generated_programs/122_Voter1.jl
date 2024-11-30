using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("area code state_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("area code state_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["contestant number", "state"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "area code"], Any[1, "contestant name"], Any[2, "vote id"], Any[2, "phone number"], Any[2, "created"]]
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





PClean.@model Voter1Model begin
    @class Area_Code_State begin
        area_code ~ ChooseUniformly(possibilities[:area_code])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Contestants begin
        contestant_number ~ ChooseUniformly(possibilities[:contestant_number])
        contestant_name ~ ChooseUniformly(possibilities[:contestant_name])
    end

    @class Obs begin
        area_Code_State ~ Area_Code_State
        contestants ~ Contestants
        vote_id ~ Unmodeled()
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        created ~ TimePrior(possibilities[:created])
    end
end

query = @query Voter1Model.Obs [
    area_code_state_area_code area_Code_State.area_code
    area_code_state_state area_Code_State.state
    contestants_contestant_number contestants.contestant_number
    contestants_contestant_name contestants.contestant_name
    votes_vote_id vote_id
    votes_phone_number phone_number
    votes_created created
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
