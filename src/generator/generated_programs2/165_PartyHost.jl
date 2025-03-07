using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("party_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("party_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "party id"], Any[0, "party theme"], Any[0, "location"], Any[0, "first year"], Any[0, "last year"], Any[0, "number of hosts"], Any[1, "host id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[2, "party id"], Any[2, "host id"], Any[2, "is main in charge"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "party id"], Any[0, "party theme"], Any[0, "location"], Any[0, "first year"], Any[0, "last year"], Any[0, "number of hosts"], Any[1, "host id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[2, "party id"], Any[2, "host id"], Any[2, "is main in charge"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "party id"], Any[0, "party theme"], Any[0, "location"], Any[0, "first year"], Any[0, "last year"], Any[0, "number of hosts"], Any[1, "host id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[2, "party id"], Any[2, "host id"], Any[2, "is main in charge"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "party id"], Any[0, "party theme"], Any[0, "location"], Any[0, "first year"], Any[0, "last year"], Any[0, "number of hosts"], Any[1, "host id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[2, "party id"], Any[2, "host id"], Any[2, "is main in charge"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "party id"], Any[0, "party theme"], Any[0, "location"], Any[0, "first year"], Any[0, "last year"], Any[0, "number of hosts"], Any[1, "host id"], Any[1, "name"], Any[1, "nationality"], Any[1, "age"], Any[2, "party id"], Any[2, "host id"], Any[2, "is main in charge"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 1], Any[12, 7]])
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







PClean.@model PartyHostModel begin
    @class Party begin
        party_theme ~ ChooseUniformly(possibilities[:party_theme])
        location ~ ChooseUniformly(possibilities[:location])
        first_year ~ ChooseUniformly(possibilities[:first_year])
        last_year ~ ChooseUniformly(possibilities[:last_year])
        number_of_hosts ~ ChooseUniformly(possibilities[:number_of_hosts])
    end

    @class Host begin
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Party_host begin
        host ~ Host
        is_main_in_charge ~ ChooseUniformly(possibilities[:is_main_in_charge])
    end

    @class Obs begin
        party_host ~ Party_host
    end
end

query = @query PartyHostModel.Obs [
    party_id party_host.party.party_id
    party_theme party_host.party.party_theme
    party_location party_host.party.location
    party_first_year party_host.party.first_year
    party_last_year party_host.party.last_year
    party_number_of_hosts party_host.party.number_of_hosts
    host_id party_host.host.host_id
    host_name party_host.host.name
    host_nationality party_host.host.nationality
    host_age party_host.host.age
    party_host_is_main_in_charge party_host.is_main_in_charge
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
