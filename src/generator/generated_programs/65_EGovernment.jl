using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "town city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "service id"], Any[1, "service type code"], Any[1, "service name"], Any[1, "service descriptio"], Any[2, "form id"], Any[2, "form type code"], Any[2, "service id"], Any[2, "form number"], Any[2, "form name"], Any[2, "form description"], Any[3, "individual id"], Any[3, "individual first name"], Any[3, "individual middle name"], Any[3, "inidividual phone"], Any[3, "individual email"], Any[3, "individual address"], Any[3, "individual last name"], Any[4, "organization id"], Any[4, "date formed"], Any[4, "organization name"], Any[4, "uk vat number"], Any[5, "party id"], Any[5, "payment method code"], Any[5, "party phone"], Any[5, "party email"], Any[6, "individual id"], Any[6, "organization id"], Any[6, "date contact from"], Any[6, "date contact to"], Any[7, "party id"], Any[7, "address id"], Any[7, "date address from"], Any[7, "address type code"], Any[7, "date address to"], Any[8, "party id"], Any[8, "form id"], Any[8, "date completion started"], Any[8, "form status code"], Any[8, "date fully completed"], Any[9, "booking id"], Any[9, "customer id"], Any[9, "service id"], Any[9, "service datetime"], Any[9, "booking made date"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "town city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "service id"], Any[1, "service type code"], Any[1, "service name"], Any[1, "service descriptio"], Any[2, "form id"], Any[2, "form type code"], Any[2, "service id"], Any[2, "form number"], Any[2, "form name"], Any[2, "form description"], Any[3, "individual id"], Any[3, "individual first name"], Any[3, "individual middle name"], Any[3, "inidividual phone"], Any[3, "individual email"], Any[3, "individual address"], Any[3, "individual last name"], Any[4, "organization id"], Any[4, "date formed"], Any[4, "organization name"], Any[4, "uk vat number"], Any[5, "party id"], Any[5, "payment method code"], Any[5, "party phone"], Any[5, "party email"], Any[6, "individual id"], Any[6, "organization id"], Any[6, "date contact from"], Any[6, "date contact to"], Any[7, "party id"], Any[7, "address id"], Any[7, "date address from"], Any[7, "address type code"], Any[7, "date address to"], Any[8, "party id"], Any[8, "form id"], Any[8, "date completion started"], Any[8, "form status code"], Any[8, "date fully completed"], Any[9, "booking id"], Any[9, "customer id"], Any[9, "service id"], Any[9, "service datetime"], Any[9, "booking made date"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model EGovernmentModel begin
    @class Addresses begin
        address_id ~ Unmodeled()
        line_1_number_building ~ ChooseUniformly(possibilities[:line_1_number_building])
        town_city ~ ChooseUniformly(possibilities[:town_city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Services begin
        service_id ~ Unmodeled()
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
        service_name ~ ChooseUniformly(possibilities[:service_name])
        service_descriptio ~ ChooseUniformly(possibilities[:service_descriptio])
    end

    @class Forms begin
        form_id ~ Unmodeled()
        form_type_code ~ ChooseUniformly(possibilities[:form_type_code])
        service_id ~ ChooseUniformly(possibilities[:service_id])
        form_number ~ ChooseUniformly(possibilities[:form_number])
        form_name ~ ChooseUniformly(possibilities[:form_name])
        form_description ~ ChooseUniformly(possibilities[:form_description])
    end

    @class Individuals begin
        individual_id ~ Unmodeled()
        individual_first_name ~ ChooseUniformly(possibilities[:individual_first_name])
        individual_middle_name ~ ChooseUniformly(possibilities[:individual_middle_name])
        inidividual_phone ~ ChooseUniformly(possibilities[:inidividual_phone])
        individual_email ~ ChooseUniformly(possibilities[:individual_email])
        individual_address ~ ChooseUniformly(possibilities[:individual_address])
        individual_last_name ~ ChooseUniformly(possibilities[:individual_last_name])
    end

    @class Organizations begin
        organization_id ~ Unmodeled()
        date_formed ~ TimePrior(possibilities[:date_formed])
        organization_name ~ ChooseUniformly(possibilities[:organization_name])
        uk_vat_number ~ ChooseUniformly(possibilities[:uk_vat_number])
    end

    @class Parties begin
        party_id ~ Unmodeled()
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        party_phone ~ ChooseUniformly(possibilities[:party_phone])
        party_email ~ ChooseUniformly(possibilities[:party_email])
    end

    @class Organization_Contact_Individuals begin
        individual_id ~ Unmodeled()
        organization_id ~ ChooseUniformly(possibilities[:organization_id])
        date_contact_from ~ TimePrior(possibilities[:date_contact_from])
        date_contact_to ~ TimePrior(possibilities[:date_contact_to])
    end

    @class Party_Addresses begin
        party_id ~ Unmodeled()
        address_id ~ ChooseUniformly(possibilities[:address_id])
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        address_type_code ~ ChooseUniformly(possibilities[:address_type_code])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
    end

    @class Party_Forms begin
        party_id ~ Unmodeled()
        form_id ~ ChooseUniformly(possibilities[:form_id])
        date_completion_started ~ TimePrior(possibilities[:date_completion_started])
        form_status_code ~ ChooseUniformly(possibilities[:form_status_code])
        date_fully_completed ~ TimePrior(possibilities[:date_fully_completed])
    end

    @class Party_Services begin
        booking_id ~ Unmodeled()
        customer_id ~ ChooseUniformly(possibilities[:customer_id])
        service_id ~ ChooseUniformly(possibilities[:service_id])
        service_datetime ~ TimePrior(possibilities[:service_datetime])
        booking_made_date ~ TimePrior(possibilities[:booking_made_date])
    end

    @class Obs begin
        addresses ~ Addresses
        services ~ Services
        forms ~ Forms
        individuals ~ Individuals
        organizations ~ Organizations
        parties ~ Parties
        organization_Contact_Individuals ~ Organization_Contact_Individuals
        party_Addresses ~ Party_Addresses
        party_Forms ~ Party_Forms
        party_Services ~ Party_Services
    end
end

query = @query EGovernmentModel.Obs [
    addresses_address_id addresses.address_id
    addresses_line_1_number_building addresses.line_1_number_building
    addresses_town_city addresses.town_city
    addresses_zip_postcode addresses.zip_postcode
    addresses_state_province_county addresses.state_province_county
    addresses_country addresses.country
    services_service_id services.service_id
    services_service_type_code services.service_type_code
    services_service_name services.service_name
    services_service_descriptio services.service_descriptio
    forms_form_id forms.form_id
    forms_form_type_code forms.form_type_code
    forms_form_number forms.form_number
    forms_form_name forms.form_name
    forms_form_description forms.form_description
    individuals_individual_id individuals.individual_id
    individuals_individual_first_name individuals.individual_first_name
    individuals_individual_middle_name individuals.individual_middle_name
    individuals_inidividual_phone individuals.inidividual_phone
    individuals_individual_email individuals.individual_email
    individuals_individual_address individuals.individual_address
    individuals_individual_last_name individuals.individual_last_name
    organizations_organization_id organizations.organization_id
    organizations_date_formed organizations.date_formed
    organizations_organization_name organizations.organization_name
    organizations_uk_vat_number organizations.uk_vat_number
    parties_party_id parties.party_id
    parties_payment_method_code parties.payment_method_code
    parties_party_phone parties.party_phone
    parties_party_email parties.party_email
    organization_contact_individuals_date_contact_from organization_Contact_Individuals.date_contact_from
    organization_contact_individuals_date_contact_to organization_Contact_Individuals.date_contact_to
    party_addresses_date_address_from party_Addresses.date_address_from
    party_addresses_address_type_code party_Addresses.address_type_code
    party_addresses_date_address_to party_Addresses.date_address_to
    party_forms_date_completion_started party_Forms.date_completion_started
    party_forms_form_status_code party_Forms.form_status_code
    party_forms_date_fully_completed party_Forms.date_fully_completed
    party_services_booking_id party_Services.booking_id
    party_services_service_datetime party_Services.service_datetime
    party_services_booking_made_date party_Services.booking_made_date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
