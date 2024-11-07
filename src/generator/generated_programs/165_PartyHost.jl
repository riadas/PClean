using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("party_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("party_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "party id"], Any[0, "party theme"], Any[0, "location"], Any[0, "first year"], Any[0, "last year"], Any[0, "number of hosts"], Any[1, "host id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[2, "party id"], Any[2, "host id"], Any[2, "is main in charge"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "party id"], Any[0, "party theme"], Any[0, "location"], Any[0, "first year"], Any[0, "last year"], Any[0, "number of hosts"], Any[1, "host id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[2, "party id"], Any[2, "host id"], Any[2, "is main in charge"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model PartyHostModel begin
    @class Party begin
        party_id ~ Unmodeled()
        party_theme ~ ChooseUniformly(possibilities[:party_theme])
        location ~ ChooseUniformly(possibilities[:location])
        first_year ~ ChooseUniformly(possibilities[:first_year])
        last_year ~ ChooseUniformly(possibilities[:last_year])
        number_of_hosts ~ ChooseUniformly(possibilities[:number_of_hosts])
    end

    @class Host begin
        host_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Party_Host begin
        party_id ~ Unmodeled()
        host_id ~ ChooseUniformly(possibilities[:host_id])
        is_main_in_charge ~ ChooseUniformly(possibilities[:is_main_in_charge])
    end

    @class Obs begin
        party ~ Party
        host ~ Host
        party_Host ~ Party_Host
    end
end

query = @query PartyHostModel.Obs [
    party_id party.party_id
    party_theme party.party_theme
    party_location party.location
    party_first_year party.first_year
    party_last_year party.last_year
    party_number_of_hosts party.number_of_hosts
    host_id host.host_id
    host_name host.name
    host_nationality host.nationality
    host_age host.age
    party_host_is_main_in_charge party_Host.is_main_in_charge
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
