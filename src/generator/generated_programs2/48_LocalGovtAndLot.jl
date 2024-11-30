using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("customers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("customers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "property id"], Any[1, "property type code"], Any[1, "property address"], Any[1, "other details"], Any[2, "resident id"], Any[2, "property id"], Any[2, "date moved in"], Any[2, "date moved out"], Any[2, "other details"], Any[3, "organization id"], Any[3, "parent organization id"], Any[3, "organization details"], Any[4, "service id"], Any[4, "organization id"], Any[4, "service type code"], Any[4, "service details"], Any[5, "resident id"], Any[5, "service id"], Any[5, "date moved in"], Any[5, "property id"], Any[5, "date requested"], Any[5, "date provided"], Any[5, "other details"], Any[6, "thing id"], Any[6, "organization id"], Any[6, "type of thing code"], Any[6, "service type code"], Any[6, "service details"], Any[7, "customer event id"], Any[7, "customer id"], Any[7, "date moved in"], Any[7, "property id"], Any[7, "resident id"], Any[7, "thing id"], Any[8, "customer event note id"], Any[8, "customer event id"], Any[8, "service type code"], Any[8, "resident id"], Any[8, "property id"], Any[8, "date moved in"], Any[9, "thing id"], Any[9, "date and date"], Any[9, "status of thing code"], Any[10, "thing id"], Any[10, "date and time"], Any[10, "location code"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "customer id"], Any[0, "customer details"], Any[1, "property id"], Any[1, "property type code"], Any[1, "property address"], Any[1, "other details"], Any[2, "resident id"], Any[2, "property id"], Any[2, "date moved in"], Any[2, "date moved out"], Any[2, "other details"], Any[3, "organization id"], Any[3, "parent organization id"], Any[3, "organization details"], Any[4, "service id"], Any[4, "organization id"], Any[4, "service type code"], Any[4, "service details"], Any[5, "resident id"], Any[5, "service id"], Any[5, "date moved in"], Any[5, "property id"], Any[5, "date requested"], Any[5, "date provided"], Any[5, "other details"], Any[6, "thing id"], Any[6, "organization id"], Any[6, "type of thing code"], Any[6, "service type code"], Any[6, "service details"], Any[7, "customer event id"], Any[7, "customer id"], Any[7, "date moved in"], Any[7, "property id"], Any[7, "resident id"], Any[7, "thing id"], Any[8, "customer event note id"], Any[8, "customer event id"], Any[8, "service type code"], Any[8, "resident id"], Any[8, "property id"], Any[8, "date moved in"], Any[9, "thing id"], Any[9, "date and date"], Any[9, "status of thing code"], Any[10, "thing id"], Any[10, "date and time"], Any[10, "location code"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["property id", "organization id", "resident id", "property id", "date moved in", "service id", "organization id", "resident id", "property id", "date moved in", "customer id", "thing id", "customer event id", "thing id", "thing id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "customer details"], Any[1, "property type code"], Any[1, "property address"], Any[1, "other details"], Any[2, "date moved out"], Any[2, "other details"], Any[3, "parent organization id"], Any[3, "organization details"], Any[4, "service type code"], Any[4, "service details"], Any[5, "date requested"], Any[5, "date provided"], Any[5, "other details"], Any[6, "type of thing code"], Any[6, "service type code"], Any[6, "service details"], Any[8, "customer event note id"], Any[8, "service type code"], Any[9, "date and date"], Any[9, "status of thing code"], Any[10, "date and time"], Any[10, "location code"]]
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





PClean.@model LocalGovtAndLotModel begin
    @class Customers begin
        customer_id ~ Unmodeled()
        customer_details ~ ChooseUniformly(possibilities[:customer_details])
    end

    @class Properties begin
        property_id ~ Unmodeled()
        property_type_code ~ ChooseUniformly(possibilities[:property_type_code])
        property_address ~ ChooseUniformly(possibilities[:property_address])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Residents begin
        resident_id ~ Unmodeled()
        properties ~ Properties
        date_moved_in ~ TimePrior(possibilities[:date_moved_in])
        date_moved_out ~ TimePrior(possibilities[:date_moved_out])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Organizations begin
        organization_id ~ Unmodeled()
        parent_organization_id ~ ChooseUniformly(possibilities[:parent_organization_id])
        organization_details ~ ChooseUniformly(possibilities[:organization_details])
    end

    @class Services begin
        service_id ~ Unmodeled()
        organizations ~ Organizations
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
        service_details ~ ChooseUniformly(possibilities[:service_details])
    end

    @class Residents_Services begin
        residents ~ Residents
        services ~ Services
        residents ~ Residents
        residents ~ Residents
        date_requested ~ TimePrior(possibilities[:date_requested])
        date_provided ~ TimePrior(possibilities[:date_provided])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Things begin
        thing_id ~ Unmodeled()
        organizations ~ Organizations
        type_of_thing_code ~ ChooseUniformly(possibilities[:type_of_thing_code])
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
        service_details ~ ChooseUniformly(possibilities[:service_details])
    end

    @class Customer_Events begin
        customer_event_id ~ Unmodeled()
        customers ~ Customers
        residents ~ Residents
        residents ~ Residents
        residents ~ Residents
        things ~ Things
    end

    @class Customer_Event_Notes begin
        customer_event_note_id ~ Unmodeled()
        customer_Events ~ Customer_Events
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
        resident_id ~ ChooseUniformly(possibilities[:resident_id])
        property_id ~ ChooseUniformly(possibilities[:property_id])
        date_moved_in ~ TimePrior(possibilities[:date_moved_in])
    end

    @class Timed_Status_Of_Things begin
        things ~ Things
        date_and_date ~ TimePrior(possibilities[:date_and_date])
        status_of_thing_code ~ ChooseUniformly(possibilities[:status_of_thing_code])
    end

    @class Timed_Locations_Of_Things begin
        things ~ Things
        date_and_time ~ TimePrior(possibilities[:date_and_time])
        location_code ~ ChooseUniformly(possibilities[:location_code])
    end

    @class Obs begin
        residents_Services ~ Residents_Services
        customer_Event_Notes ~ Customer_Event_Notes
        timed_Status_Of_Things ~ Timed_Status_Of_Things
        timed_Locations_Of_Things ~ Timed_Locations_Of_Things
    end
end

query = @query LocalGovtAndLotModel.Obs [
    customers_customer_id customer_Event_Notes.customer_Events.customers.customer_id
    customers_customer_details customer_Event_Notes.customer_Events.customers.customer_details
    properties_property_id residents_Services.residents.properties.property_id
    properties_property_type_code residents_Services.residents.properties.property_type_code
    properties_property_address residents_Services.residents.properties.property_address
    properties_other_details residents_Services.residents.properties.other_details
    residents_resident_id residents_Services.residents.resident_id
    residents_date_moved_in residents_Services.residents.date_moved_in
    residents_date_moved_out residents_Services.residents.date_moved_out
    residents_other_details residents_Services.residents.other_details
    organizations_organization_id residents_Services.services.organizations.organization_id
    organizations_parent_organization_id residents_Services.services.organizations.parent_organization_id
    organizations_organization_details residents_Services.services.organizations.organization_details
    services_service_id residents_Services.services.service_id
    services_service_type_code residents_Services.services.service_type_code
    services_service_details residents_Services.services.service_details
    residents_services_date_requested residents_Services.date_requested
    residents_services_date_provided residents_Services.date_provided
    residents_services_other_details residents_Services.other_details
    things_thing_id timed_Status_Of_Things.things.thing_id
    things_type_of_thing_code timed_Status_Of_Things.things.type_of_thing_code
    things_service_type_code timed_Status_Of_Things.things.service_type_code
    things_service_details timed_Status_Of_Things.things.service_details
    customer_events_customer_event_id customer_Event_Notes.customer_Events.customer_event_id
    customer_event_notes_customer_event_note_id customer_Event_Notes.customer_event_note_id
    customer_event_notes_service_type_code customer_Event_Notes.service_type_code
    customer_event_notes_resident_id customer_Event_Notes.resident_id
    customer_event_notes_property_id customer_Event_Notes.property_id
    customer_event_notes_date_moved_in customer_Event_Notes.date_moved_in
    timed_status_of_things_date_and_date timed_Status_Of_Things.date_and_date
    timed_status_of_things_status_of_thing_code timed_Status_Of_Things.status_of_thing_code
    timed_locations_of_things_date_and_time timed_Locations_Of_Things.date_and_time
    timed_locations_of_things_location_code timed_Locations_Of_Things.location_code
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
