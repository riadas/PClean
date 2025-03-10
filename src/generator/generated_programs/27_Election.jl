using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("county_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("county_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["district", "party"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "county id"], Any[0, "county name"], Any[0, "population"], Any[0, "zip code"], Any[1, "party id"], Any[1, "year"], Any[1, "governor"], Any[1, "lieutenant governor"], Any[1, "comptroller"], Any[1, "attorney general"], Any[1, "us senate"], Any[2, "election id"], Any[2, "counties represented"], Any[2, "delegate"], Any[2, "first elected"], Any[2, "committee"]]
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





PClean.@model ElectionModel begin
    @class County begin
        county_id ~ Unmodeled()
        county_name ~ ChooseUniformly(possibilities[:county_name])
        population ~ ChooseUniformly(possibilities[:population])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
    end

    @class Party begin
        party_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        party ~ ChooseUniformly(possibilities[:party])
        governor ~ ChooseUniformly(possibilities[:governor])
        lieutenant_governor ~ ChooseUniformly(possibilities[:lieutenant_governor])
        comptroller ~ ChooseUniformly(possibilities[:comptroller])
        attorney_general ~ ChooseUniformly(possibilities[:attorney_general])
        us_senate ~ ChooseUniformly(possibilities[:us_senate])
    end

    @class Obs begin
        county ~ County
        party ~ Party
        election_id ~ Unmodeled()
        counties_represented ~ ChooseUniformly(possibilities[:counties_represented])
        delegate ~ ChooseUniformly(possibilities[:delegate])
        first_elected ~ ChooseUniformly(possibilities[:first_elected])
        committee ~ ChooseUniformly(possibilities[:committee])
    end
end

query = @query ElectionModel.Obs [
    county_id county.county_id
    county_name county.county_name
    county_population county.population
    county_zip_code county.zip_code
    party_id party.party_id
    party_year party.year
    party party.party
    party_governor party.governor
    party_lieutenant_governor party.lieutenant_governor
    party_comptroller party.comptroller
    party_attorney_general party.attorney_general
    party_us_senate party.us_senate
    election_id election_id
    election_counties_represented counties_represented
    election_delegate delegate
    election_first_elected first_elected
    election_committee committee
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
