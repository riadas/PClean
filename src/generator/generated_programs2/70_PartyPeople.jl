using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("region_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("region_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "party id"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "region id"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "party id"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"], Any[3, "party id"], Any[3, "member in charge id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "party id"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "region id"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "party id"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"], Any[3, "party id"], Any[3, "member in charge id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "party id"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "region id"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "party id"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"], Any[3, "party id"], Any[3, "member in charge id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "party id"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "region id"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "party id"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"], Any[3, "party id"], Any[3, "member in charge id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "region id"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "party id"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "region id"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "party id"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"], Any[3, "party id"], Any[3, "member in charge id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 1], Any[15, 7], Any[20, 13], Any[19, 7]])
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







PClean.@model PartyPeopleModel begin
    @class Region begin
        region_name ~ ChooseUniformly(possibilities[:region_name])
        date ~ ChooseUniformly(possibilities[:date])
        label ~ ChooseUniformly(possibilities[:label])
        format ~ ChooseUniformly(possibilities[:format])
        catalogue ~ ChooseUniformly(possibilities[:catalogue])
    end

    @class Party begin
        minister ~ ChooseUniformly(possibilities[:minister])
        took_office ~ ChooseUniformly(possibilities[:took_office])
        left_office ~ ChooseUniformly(possibilities[:left_office])
        region ~ Region
        party_name ~ ChooseUniformly(possibilities[:party_name])
    end

    @class Member begin
        member_name ~ ChooseUniformly(possibilities[:member_name])
        party ~ Party
        in_office ~ ChooseUniformly(possibilities[:in_office])
    end

    @class Party_events begin
        event_name ~ ChooseUniformly(possibilities[:event_name])
        party ~ Party
        member ~ Member
    end

    @class Obs begin
        party_events ~ Party_events
    end
end

query = @query PartyPeopleModel.Obs [
    region_id party_events.member.party.region.region_id
    region_name party_events.member.party.region.region_name
    region_date party_events.member.party.region.date
    region_label party_events.member.party.region.label
    region_format party_events.member.party.region.format
    region_catalogue party_events.member.party.region.catalogue
    party_id party_events.member.party.party_id
    party_minister party_events.member.party.minister
    party_took_office party_events.member.party.took_office
    party_left_office party_events.member.party.left_office
    party_name party_events.member.party.party_name
    member_id party_events.member.member_id
    member_name party_events.member.member_name
    member_in_office party_events.member.in_office
    party_events_event_id party_events.event_id
    party_events_event_name party_events.event_name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
