using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("region_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("region_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "party id"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "region id"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "party id"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"], Any[3, "party id"], Any[3, "member in charge id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "party id"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "region id"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "party id"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"], Any[3, "party id"], Any[3, "member in charge id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model PartyPeopleModel begin
    @class Region begin
        region_id ~ Unmodeled()
        region_name ~ ChooseUniformly(possibilities[:region_name])
        date ~ ChooseUniformly(possibilities[:date])
        label ~ ChooseUniformly(possibilities[:label])
        format ~ ChooseUniformly(possibilities[:format])
        catalogue ~ ChooseUniformly(possibilities[:catalogue])
    end

    @class Party begin
        party_id ~ Unmodeled()
        minister ~ ChooseUniformly(possibilities[:minister])
        took_office ~ ChooseUniformly(possibilities[:took_office])
        left_office ~ ChooseUniformly(possibilities[:left_office])
        region_id ~ ChooseUniformly(possibilities[:region_id])
        party_name ~ ChooseUniformly(possibilities[:party_name])
    end

    @class Member begin
        member_id ~ Unmodeled()
        member_name ~ ChooseUniformly(possibilities[:member_name])
        party_id ~ ChooseUniformly(possibilities[:party_id])
        in_office ~ ChooseUniformly(possibilities[:in_office])
    end

    @class Party_Events begin
        event_id ~ Unmodeled()
        event_name ~ ChooseUniformly(possibilities[:event_name])
        party_id ~ ChooseUniformly(possibilities[:party_id])
        member_in_charge_id ~ ChooseUniformly(possibilities[:member_in_charge_id])
    end

    @class Obs begin
        region ~ Region
        party ~ Party
        member ~ Member
        party_Events ~ Party_Events
    end
end

query = @query PartyPeopleModel.Obs [
    region_id region.region_id
    region_name region.region_name
    region_date region.date
    region_label region.label
    region_format region.format
    region_catalogue region.catalogue
    party_id party.party_id
    party_minister party.minister
    party_took_office party.took_office
    party_left_office party.left_office
    party_name party.party_name
    member_id member.member_id
    member_name member.member_name
    member_in_office member.in_office
    party_events_event_id party_Events.event_id
    party_events_event_name party_Events.event_name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
