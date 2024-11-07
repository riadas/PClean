using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("services_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("services_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "service id"], Any[0, "service type code"], Any[1, "participant id"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event id"], Any[2, "service id"], Any[2, "event details"], Any[3, "event id"], Any[3, "participant id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "service id"], Any[0, "service type code"], Any[1, "participant id"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event id"], Any[2, "service id"], Any[2, "event details"], Any[3, "event id"], Any[3, "participant id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model LocalGovtInAlabamaModel begin
    @class Services begin
        service_id ~ Unmodeled()
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
    end

    @class Participants begin
        participant_id ~ Unmodeled()
        participant_type_code ~ ChooseUniformly(possibilities[:participant_type_code])
        participant_details ~ ChooseUniformly(possibilities[:participant_details])
    end

    @class Events begin
        event_id ~ Unmodeled()
        service_id ~ ChooseUniformly(possibilities[:service_id])
        event_details ~ ChooseUniformly(possibilities[:event_details])
    end

    @class Participants_In_Events begin
        event_id ~ Unmodeled()
        participant_id ~ ChooseUniformly(possibilities[:participant_id])
    end

    @class Obs begin
        services ~ Services
        participants ~ Participants
        events ~ Events
        participants_In_Events ~ Participants_In_Events
    end
end

query = @query LocalGovtInAlabamaModel.Obs [
    services_service_id services.service_id
    services_service_type_code services.service_type_code
    participants_participant_id participants.participant_id
    participants_participant_type_code participants.participant_type_code
    participants_participant_details participants.participant_details
    events_event_id events.event_id
    events_event_details events.event_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
