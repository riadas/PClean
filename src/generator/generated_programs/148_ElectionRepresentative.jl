using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("election_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("election_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "election id"], Any[0, "representative id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "representative id"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "election id"], Any[0, "representative id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "representative id"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ElectionRepresentativeModel begin
    @class Election begin
        election_id ~ Unmodeled()
        representative_id ~ ChooseUniformly(possibilities[:representative_id])
        date ~ ChooseUniformly(possibilities[:date])
        votes ~ ChooseUniformly(possibilities[:votes])
        vote_percent ~ ChooseUniformly(possibilities[:vote_percent])
        seats ~ ChooseUniformly(possibilities[:seats])
        place ~ ChooseUniformly(possibilities[:place])
    end

    @class Representative begin
        representative_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        state ~ ChooseUniformly(possibilities[:state])
        party ~ ChooseUniformly(possibilities[:party])
        lifespan ~ ChooseUniformly(possibilities[:lifespan])
    end

    @class Obs begin
        election ~ Election
        representative ~ Representative
    end
end

query = @query ElectionRepresentativeModel.Obs [
    election_id election.election_id
    election_date election.date
    election_votes election.votes
    election_vote_percent election.vote_percent
    election_seats election.seats
    election_place election.place
    representative_id representative.representative_id
    representative_name representative.name
    representative_state representative.state
    representative_party representative.party
    representative_lifespan representative.lifespan
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
