using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("region_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("region_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["region id", "party id", "member in charge id", "party id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "region name"], Any[0, "date"], Any[0, "label"], Any[0, "format"], Any[0, "catalogue"], Any[1, "minister"], Any[1, "took office"], Any[1, "left office"], Any[1, "party name"], Any[2, "member id"], Any[2, "member name"], Any[2, "in office"], Any[3, "event id"], Any[3, "event name"]]
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





PClean.@model PartyPeopleModel begin
    @class Region begin
        region_id ~ Unmodeled()
        region_name ~ ChooseUniformly(possibilities[:region_name])
        date ~ ChooseUniformly(possibilities[:date])
        label ~ ChooseUniformly(possibilities[:label])
        format ~ ChooseUniformly(possibilities[:format])
        catalogue ~ ChooseUniformly(possibilities[:catalogue])
    end

    @class Obs begin
        region ~ Region
        party_id ~ Unmodeled()
        minister ~ ChooseUniformly(possibilities[:minister])
        took_office ~ ChooseUniformly(possibilities[:took_office])
        left_office ~ ChooseUniformly(possibilities[:left_office])
        party_name ~ ChooseUniformly(possibilities[:party_name])
        member_id ~ Unmodeled()
        member_name ~ ChooseUniformly(possibilities[:member_name])
        in_office ~ ChooseUniformly(possibilities[:in_office])
        event_id ~ Unmodeled()
        event_name ~ ChooseUniformly(possibilities[:event_name])
    end
end

query = @query PartyPeopleModel.Obs [
    region_id region.region_id
    region_name region.region_name
    region_date region.date
    region_label region.label
    region_format region.format
    region_catalogue region.catalogue
    party_id party_id
    party_minister minister
    party_took_office took_office
    party_left_office left_office
    party_name party_name
    member_id member_id
    member_name member_name
    member_in_office in_office
    party_events_event_id event_id
    party_events_event_name event_name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
