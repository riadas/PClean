using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("county_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("county_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "county id"], Any[0, "county name"], Any[0, "population"], Any[0, "zip code"], Any[1, "party id"], Any[1, "year"], Any[1, "party"], Any[1, "governor"], Any[1, "lieutenant governor"], Any[1, "comptroller"], Any[1, "attorney general"], Any[1, "us senate"], Any[2, "election id"], Any[2, "counties represented"], Any[2, "district"], Any[2, "delegate"], Any[2, "party"], Any[2, "first elected"], Any[2, "committee"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "county id"], Any[0, "county name"], Any[0, "population"], Any[0, "zip code"], Any[1, "party id"], Any[1, "year"], Any[1, "party"], Any[1, "governor"], Any[1, "lieutenant governor"], Any[1, "comptroller"], Any[1, "attorney general"], Any[1, "us senate"], Any[2, "election id"], Any[2, "counties represented"], Any[2, "district"], Any[2, "delegate"], Any[2, "party"], Any[2, "first elected"], Any[2, "committee"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "county id"], Any[0, "county name"], Any[0, "population"], Any[0, "zip code"], Any[1, "party id"], Any[1, "year"], Any[1, "party"], Any[1, "governor"], Any[1, "lieutenant governor"], Any[1, "comptroller"], Any[1, "attorney general"], Any[1, "us senate"], Any[2, "election id"], Any[2, "counties represented"], Any[2, "district"], Any[2, "delegate"], Any[2, "party"], Any[2, "first elected"], Any[2, "committee"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "county id"], Any[0, "county name"], Any[0, "population"], Any[0, "zip code"], Any[1, "party id"], Any[1, "year"], Any[1, "party"], Any[1, "governor"], Any[1, "lieutenant governor"], Any[1, "comptroller"], Any[1, "attorney general"], Any[1, "us senate"], Any[2, "election id"], Any[2, "counties represented"], Any[2, "district"], Any[2, "delegate"], Any[2, "party"], Any[2, "first elected"], Any[2, "committee"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "county id"], Any[0, "county name"], Any[0, "population"], Any[0, "zip code"], Any[1, "party id"], Any[1, "year"], Any[1, "party"], Any[1, "governor"], Any[1, "lieutenant governor"], Any[1, "comptroller"], Any[1, "attorney general"], Any[1, "us senate"], Any[2, "election id"], Any[2, "counties represented"], Any[2, "district"], Any[2, "delegate"], Any[2, "party"], Any[2, "first elected"], Any[2, "committee"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[15, 1], Any[17, 5]])
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







PClean.@model ElectionModel begin
    @class County begin
        county_name ~ ChooseUniformly(possibilities[:county_name])
        population ~ ChooseUniformly(possibilities[:population])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
    end

    @class Party begin
        year ~ ChooseUniformly(possibilities[:year])
        party ~ ChooseUniformly(possibilities[:party])
        governor ~ ChooseUniformly(possibilities[:governor])
        lieutenant_governor ~ ChooseUniformly(possibilities[:lieutenant_governor])
        comptroller ~ ChooseUniformly(possibilities[:comptroller])
        attorney_general ~ ChooseUniformly(possibilities[:attorney_general])
        us_senate ~ ChooseUniformly(possibilities[:us_senate])
    end

    @class Election begin
        counties_represented ~ ChooseUniformly(possibilities[:counties_represented])
        county ~ County
        delegate ~ ChooseUniformly(possibilities[:delegate])
        party ~ Party
        first_elected ~ ChooseUniformly(possibilities[:first_elected])
        committee ~ ChooseUniformly(possibilities[:committee])
    end

    @class Obs begin
        election ~ Election
    end
end

query = @query ElectionModel.Obs [
    county_id election.county.county_id
    county_name election.county.county_name
    county_population election.county.population
    county_zip_code election.county.zip_code
    party_id election.party.party_id
    party_year election.party.year
    party election.party.party
    party_governor election.party.governor
    party_lieutenant_governor election.party.lieutenant_governor
    party_comptroller election.party.comptroller
    party_attorney_general election.party.attorney_general
    party_us_senate election.party.us_senate
    election_id election.election_id
    election_counties_represented election.counties_represented
    election_delegate election.delegate
    election_first_elected election.first_elected
    election_committee election.committee
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
