using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("addresses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("addresses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "town city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "service id"], Any[1, "service type code"], Any[1, "service name"], Any[1, "service descriptio"], Any[2, "form id"], Any[2, "form type code"], Any[2, "service id"], Any[2, "form number"], Any[2, "form name"], Any[2, "form description"], Any[3, "individual id"], Any[3, "individual first name"], Any[3, "individual middle name"], Any[3, "inidividual phone"], Any[3, "individual email"], Any[3, "individual address"], Any[3, "individual last name"], Any[4, "organization id"], Any[4, "date formed"], Any[4, "organization name"], Any[4, "uk vat number"], Any[5, "party id"], Any[5, "payment method code"], Any[5, "party phone"], Any[5, "party email"], Any[6, "individual id"], Any[6, "organization id"], Any[6, "date contact from"], Any[6, "date contact to"], Any[7, "party id"], Any[7, "address id"], Any[7, "date address from"], Any[7, "address type code"], Any[7, "date address to"], Any[8, "party id"], Any[8, "form id"], Any[8, "date completion started"], Any[8, "form status code"], Any[8, "date fully completed"], Any[9, "booking id"], Any[9, "customer id"], Any[9, "service id"], Any[9, "service datetime"], Any[9, "booking made date"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "town city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "service id"], Any[1, "service type code"], Any[1, "service name"], Any[1, "service descriptio"], Any[2, "form id"], Any[2, "form type code"], Any[2, "service id"], Any[2, "form number"], Any[2, "form name"], Any[2, "form description"], Any[3, "individual id"], Any[3, "individual first name"], Any[3, "individual middle name"], Any[3, "inidividual phone"], Any[3, "individual email"], Any[3, "individual address"], Any[3, "individual last name"], Any[4, "organization id"], Any[4, "date formed"], Any[4, "organization name"], Any[4, "uk vat number"], Any[5, "party id"], Any[5, "payment method code"], Any[5, "party phone"], Any[5, "party email"], Any[6, "individual id"], Any[6, "organization id"], Any[6, "date contact from"], Any[6, "date contact to"], Any[7, "party id"], Any[7, "address id"], Any[7, "date address from"], Any[7, "address type code"], Any[7, "date address to"], Any[8, "party id"], Any[8, "form id"], Any[8, "date completion started"], Any[8, "form status code"], Any[8, "date fully completed"], Any[9, "booking id"], Any[9, "customer id"], Any[9, "service id"], Any[9, "service datetime"], Any[9, "booking made date"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "town city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "service id"], Any[1, "service type code"], Any[1, "service name"], Any[1, "service descriptio"], Any[2, "form id"], Any[2, "form type code"], Any[2, "service id"], Any[2, "form number"], Any[2, "form name"], Any[2, "form description"], Any[3, "individual id"], Any[3, "individual first name"], Any[3, "individual middle name"], Any[3, "inidividual phone"], Any[3, "individual email"], Any[3, "individual address"], Any[3, "individual last name"], Any[4, "organization id"], Any[4, "date formed"], Any[4, "organization name"], Any[4, "uk vat number"], Any[5, "party id"], Any[5, "payment method code"], Any[5, "party phone"], Any[5, "party email"], Any[6, "individual id"], Any[6, "organization id"], Any[6, "date contact from"], Any[6, "date contact to"], Any[7, "party id"], Any[7, "address id"], Any[7, "date address from"], Any[7, "address type code"], Any[7, "date address to"], Any[8, "party id"], Any[8, "form id"], Any[8, "date completion started"], Any[8, "form status code"], Any[8, "date fully completed"], Any[9, "booking id"], Any[9, "customer id"], Any[9, "service id"], Any[9, "service datetime"], Any[9, "booking made date"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "town city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "service id"], Any[1, "service type code"], Any[1, "service name"], Any[1, "service descriptio"], Any[2, "form id"], Any[2, "form type code"], Any[2, "service id"], Any[2, "form number"], Any[2, "form name"], Any[2, "form description"], Any[3, "individual id"], Any[3, "individual first name"], Any[3, "individual middle name"], Any[3, "inidividual phone"], Any[3, "individual email"], Any[3, "individual address"], Any[3, "individual last name"], Any[4, "organization id"], Any[4, "date formed"], Any[4, "organization name"], Any[4, "uk vat number"], Any[5, "party id"], Any[5, "payment method code"], Any[5, "party phone"], Any[5, "party email"], Any[6, "individual id"], Any[6, "organization id"], Any[6, "date contact from"], Any[6, "date contact to"], Any[7, "party id"], Any[7, "address id"], Any[7, "date address from"], Any[7, "address type code"], Any[7, "date address to"], Any[8, "party id"], Any[8, "form id"], Any[8, "date completion started"], Any[8, "form status code"], Any[8, "date fully completed"], Any[9, "booking id"], Any[9, "customer id"], Any[9, "service id"], Any[9, "service datetime"], Any[9, "booking made date"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "address id"], Any[0, "line 1 number building"], Any[0, "town city"], Any[0, "zip postcode"], Any[0, "state province county"], Any[0, "country"], Any[1, "service id"], Any[1, "service type code"], Any[1, "service name"], Any[1, "service descriptio"], Any[2, "form id"], Any[2, "form type code"], Any[2, "service id"], Any[2, "form number"], Any[2, "form name"], Any[2, "form description"], Any[3, "individual id"], Any[3, "individual first name"], Any[3, "individual middle name"], Any[3, "inidividual phone"], Any[3, "individual email"], Any[3, "individual address"], Any[3, "individual last name"], Any[4, "organization id"], Any[4, "date formed"], Any[4, "organization name"], Any[4, "uk vat number"], Any[5, "party id"], Any[5, "payment method code"], Any[5, "party phone"], Any[5, "party email"], Any[6, "individual id"], Any[6, "organization id"], Any[6, "date contact from"], Any[6, "date contact to"], Any[7, "party id"], Any[7, "address id"], Any[7, "date address from"], Any[7, "address type code"], Any[7, "date address to"], Any[8, "party id"], Any[8, "form id"], Any[8, "date completion started"], Any[8, "form status code"], Any[8, "date fully completed"], Any[9, "booking id"], Any[9, "customer id"], Any[9, "service id"], Any[9, "service datetime"], Any[9, "booking made date"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 7], Any[32, 17], Any[33, 24], Any[36, 28], Any[37, 1], Any[42, 11], Any[41, 28], Any[47, 28], Any[48, 7]])
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







PClean.@model EGovernmentModel begin
    @class Addresses begin
        line_1_number_building ~ ChooseUniformly(possibilities[:line_1_number_building])
        town_city ~ ChooseUniformly(possibilities[:town_city])
        zip_postcode ~ ChooseUniformly(possibilities[:zip_postcode])
        state_province_county ~ ChooseUniformly(possibilities[:state_province_county])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Services begin
        service_type_code ~ ChooseUniformly(possibilities[:service_type_code])
        service_name ~ ChooseUniformly(possibilities[:service_name])
        service_descriptio ~ ChooseUniformly(possibilities[:service_descriptio])
    end

    @class Forms begin
        form_type_code ~ ChooseUniformly(possibilities[:form_type_code])
        services ~ Services
        form_number ~ ChooseUniformly(possibilities[:form_number])
        form_name ~ ChooseUniformly(possibilities[:form_name])
        form_description ~ ChooseUniformly(possibilities[:form_description])
    end

    @class Individuals begin
        individual_first_name ~ ChooseUniformly(possibilities[:individual_first_name])
        individual_middle_name ~ ChooseUniformly(possibilities[:individual_middle_name])
        inidividual_phone ~ ChooseUniformly(possibilities[:inidividual_phone])
        individual_email ~ ChooseUniformly(possibilities[:individual_email])
        individual_address ~ ChooseUniformly(possibilities[:individual_address])
        individual_last_name ~ ChooseUniformly(possibilities[:individual_last_name])
    end

    @class Organizations begin
        date_formed ~ TimePrior(possibilities[:date_formed])
        organization_name ~ ChooseUniformly(possibilities[:organization_name])
        uk_vat_number ~ ChooseUniformly(possibilities[:uk_vat_number])
    end

    @class Parties begin
        payment_method_code ~ ChooseUniformly(possibilities[:payment_method_code])
        party_phone ~ ChooseUniformly(possibilities[:party_phone])
        party_email ~ ChooseUniformly(possibilities[:party_email])
    end

    @class Organization_contact_individuals begin
        organizations ~ Organizations
        date_contact_from ~ TimePrior(possibilities[:date_contact_from])
        date_contact_to ~ TimePrior(possibilities[:date_contact_to])
    end

    @class Party_addresses begin
        addresses ~ Addresses
        date_address_from ~ TimePrior(possibilities[:date_address_from])
        address_type_code ~ ChooseUniformly(possibilities[:address_type_code])
        date_address_to ~ TimePrior(possibilities[:date_address_to])
    end

    @class Party_forms begin
        forms ~ Forms
        date_completion_started ~ TimePrior(possibilities[:date_completion_started])
        form_status_code ~ ChooseUniformly(possibilities[:form_status_code])
        date_fully_completed ~ TimePrior(possibilities[:date_fully_completed])
    end

    @class Party_services begin
        booking_id ~ Unmodeled()
        parties ~ Parties
        services ~ Services
        service_datetime ~ TimePrior(possibilities[:service_datetime])
        booking_made_date ~ TimePrior(possibilities[:booking_made_date])
    end

    @class Obs begin
        organization_contact_individuals ~ Organization_contact_individuals
        party_addresses ~ Party_addresses
        party_forms ~ Party_forms
        party_services ~ Party_services
    end
end

query = @query EGovernmentModel.Obs [
    addresses_address_id party_addresses.addresses.address_id
    addresses_line_1_number_building party_addresses.addresses.line_1_number_building
    addresses_town_city party_addresses.addresses.town_city
    addresses_zip_postcode party_addresses.addresses.zip_postcode
    addresses_state_province_county party_addresses.addresses.state_province_county
    addresses_country party_addresses.addresses.country
    services_service_id party_services.services.service_id
    services_service_type_code party_services.services.service_type_code
    services_service_name party_services.services.service_name
    services_service_descriptio party_services.services.service_descriptio
    forms_form_id party_forms.forms.form_id
    forms_form_type_code party_forms.forms.form_type_code
    forms_form_number party_forms.forms.form_number
    forms_form_name party_forms.forms.form_name
    forms_form_description party_forms.forms.form_description
    individuals_individual_id organization_contact_individuals.individuals.individual_id
    individuals_individual_first_name organization_contact_individuals.individuals.individual_first_name
    individuals_individual_middle_name organization_contact_individuals.individuals.individual_middle_name
    individuals_inidividual_phone organization_contact_individuals.individuals.inidividual_phone
    individuals_individual_email organization_contact_individuals.individuals.individual_email
    individuals_individual_address organization_contact_individuals.individuals.individual_address
    individuals_individual_last_name organization_contact_individuals.individuals.individual_last_name
    organizations_organization_id organization_contact_individuals.organizations.organization_id
    organizations_date_formed organization_contact_individuals.organizations.date_formed
    organizations_organization_name organization_contact_individuals.organizations.organization_name
    organizations_uk_vat_number organization_contact_individuals.organizations.uk_vat_number
    parties_party_id party_addresses.parties.party_id
    parties_payment_method_code party_addresses.parties.payment_method_code
    parties_party_phone party_addresses.parties.party_phone
    parties_party_email party_addresses.parties.party_email
    organization_contact_individuals_date_contact_from organization_contact_individuals.date_contact_from
    organization_contact_individuals_date_contact_to organization_contact_individuals.date_contact_to
    party_addresses_date_address_from party_addresses.date_address_from
    party_addresses_address_type_code party_addresses.address_type_code
    party_addresses_date_address_to party_addresses.date_address_to
    party_forms_date_completion_started party_forms.date_completion_started
    party_forms_form_status_code party_forms.form_status_code
    party_forms_date_fully_completed party_forms.date_fully_completed
    party_services_booking_id party_services.booking_id
    party_services_service_datetime party_services.service_datetime
    party_services_booking_made_date party_services.booking_made_date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
