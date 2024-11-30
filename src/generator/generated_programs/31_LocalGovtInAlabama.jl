using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("services_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("services_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["service id", "event id", "participant id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "service type code"], Any[1, "participant type code"], Any[1, "participant details"], Any[2, "event details"]]
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

    @class Obs begin
        services ~ Services
        participants ~ Participants
        event_id ~ Unmodeled()
        event_details ~ ChooseUniformly(possibilities[:event_details])
    end
end

query = @query LocalGovtInAlabamaModel.Obs [
    services_service_id services.service_id
    services_service_type_code services.service_type_code
    participants_participant_id participants.participant_id
    participants_participant_type_code participants.participant_type_code
    participants_participant_details participants.participant_details
    events_event_id event_id
    events_event_details event_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
