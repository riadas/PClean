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
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "people id"], Any[0, "district"], Any[0, "name"], Any[0, "party"], Any[0, "age"], Any[1, "debate id"], Any[1, "date"], Any[1, "venue"], Any[1, "num of audience"], Any[2, "debate id"], Any[2, "affirmative"], Any[2, "negative"], Any[2, "if affirmative win"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 1], Any[11, 1], Any[10, 6]])
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







PClean.@model DebateModel begin
    @class People begin
        district ~ ChooseUniformly(possibilities[:district])
        name ~ ChooseUniformly(possibilities[:name])
        party ~ ChooseUniformly(possibilities[:party])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Debate begin
        date ~ ChooseUniformly(possibilities[:date])
        venue ~ ChooseUniformly(possibilities[:venue])
        num_of_audience ~ ChooseUniformly(possibilities[:num_of_audience])
    end

    @class Debate_people begin
        people ~ People
        if_affirmative_win ~ ChooseUniformly(possibilities[:if_affirmative_win])
    end

    @class Obs begin
        debate_people ~ Debate_people
    end
end

query = @query DebateModel.Obs [
    people_id debate_people.people.people_id
    people_district debate_people.people.district
    people_name debate_people.people.name
    people_party debate_people.people.party
    people_age debate_people.people.age
    debate_id debate_people.debate.debate_id
    debate_date debate_people.debate.date
    debate_venue debate_people.debate.venue
    debate_num_of_audience debate_people.debate.num_of_audience
    debate_people_if_affirmative_win debate_people.if_affirmative_win
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
