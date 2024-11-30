using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("election_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("election_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["representative id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "election id"], Any[0, "date"], Any[0, "votes"], Any[0, "vote percent"], Any[0, "seats"], Any[0, "place"], Any[1, "name"], Any[1, "state"], Any[1, "party"], Any[1, "lifespan"]]
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





PClean.@model ElectionRepresentativeModel begin
    @class Representative begin
        representative_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        state ~ ChooseUniformly(possibilities[:state])
        party ~ ChooseUniformly(possibilities[:party])
        lifespan ~ ChooseUniformly(possibilities[:lifespan])
    end

    @class Obs begin
        representative ~ Representative
        election_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        votes ~ ChooseUniformly(possibilities[:votes])
        vote_percent ~ ChooseUniformly(possibilities[:vote_percent])
        seats ~ ChooseUniformly(possibilities[:seats])
        place ~ ChooseUniformly(possibilities[:place])
    end
end

query = @query ElectionRepresentativeModel.Obs [
    election_id election_id
    election_date date
    election_votes votes
    election_vote_percent vote_percent
    election_seats seats
    election_place place
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

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
