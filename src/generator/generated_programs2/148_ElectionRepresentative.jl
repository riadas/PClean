using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("election_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("election_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "election id"], Any[0, "representative id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "representative id"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "election id"], Any[0, "representative id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "representative id"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "election id"], Any[0, "representative id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "representative id"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "election id"], Any[0, "representative id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "representative id"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "election id"], Any[0, "representative id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "representative id"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[2, 8]])
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







PClean.@model ElectionRepresentativeModel begin
    @class Representative begin
        name ~ ChooseUniformly(possibilities[:name])
        state ~ ChooseUniformly(possibilities[:state])
        party ~ ChooseUniformly(possibilities[:party])
        lifespan ~ ChooseUniformly(possibilities[:lifespan])
    end

    @class Election begin
        representative ~ Representative
        date ~ ChooseUniformly(possibilities[:date])
        votes ~ ChooseUniformly(possibilities[:votes])
        vote_percent ~ ChooseUniformly(possibilities[:vote_percent])
        seats ~ ChooseUniformly(possibilities[:seats])
        place ~ ChooseUniformly(possibilities[:place])
    end

    @class Obs begin
        election ~ Election
    end
end

query = @query ElectionRepresentativeModel.Obs [
    election_id election.election_id
    election_date election.date
    election_votes election.votes
    election_vote_percent election.vote_percent
    election_seats election.seats
    election_place election.place
    representative_id election.representative.representative_id
    representative_name election.representative.name
    representative_state election.representative.state
    representative_party election.representative.party
    representative_lifespan election.representative.lifespan
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
