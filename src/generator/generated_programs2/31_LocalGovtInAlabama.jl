using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("services_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("services_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "service id"], Any[0, "service type code"], Any[1, "participant id"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event id"], Any[2, "service id"], Any[2, "event details"], Any[3, "event id"], Any[3, "participant id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "service id"], Any[0, "service type code"], Any[1, "participant id"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event id"], Any[2, "service id"], Any[2, "event details"], Any[3, "event id"], Any[3, "participant id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "service id"], Any[0, "service type code"], Any[1, "participant id"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event id"], Any[2, "service id"], Any[2, "event details"], Any[3, "event id"], Any[3, "participant id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "service id"], Any[0, "service type code"], Any[1, "participant id"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event id"], Any[2, "service id"], Any[2, "event details"], Any[3, "event id"], Any[3, "participant id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "service id"], Any[0, "service type code"], Any[1, "participant id"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event id"], Any[2, "service id"], Any[2, "event details"], Any[3, "event id"], Any[3, "participant id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[9, 6], Any[10, 3]])
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







PClean.@model LocalGovtInAlabamaModel begin
    @class Services begin
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
    end

    @class Participants begin
        participant_type_code ~ ChooseUniformly(possibilities[:participant_type_code])
        participant_details ~ ChooseUniformly(possibilities[:participant_details])
    end

    @class Events begin
        services ~ Services
        event_details ~ ChooseUniformly(possibilities[:event_details])
    end

    @class Participants_in_events begin
        participants ~ Participants
    end

    @class Obs begin
        participants_in_events ~ Participants_in_events
    end
end

query = @query LocalGovtInAlabamaModel.Obs [
    services_service_id participants_in_events.events.services.service_id
    services_service_type_code participants_in_events.events.services.service_type_code
    participants_participant_id participants_in_events.participants.participant_id
    participants_participant_type_code participants_in_events.participants.participant_type_code
    participants_participant_details participants_in_events.participants.participant_details
    events_event_id participants_in_events.events.event_id
    events_event_details participants_in_events.events.event_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
