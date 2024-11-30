using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("breeds_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("breeds_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "breed code"], Any[0, "breed name"], Any[1, "charge id"], Any[1, "charge type"], Any[1, "charge amount"], Any[2, "size code"], Any[2, "size description"], Any[3, "treatment type code"], Any[3, "treatment type description"], Any[4, "owner id"], Any[4, "first name"], Any[4, "last name"], Any[4, "street"], Any[4, "city"], Any[4, "state"], Any[4, "zip code"], Any[4, "email address"], Any[4, "home phone"], Any[4, "cell number"], Any[5, "dog id"], Any[5, "owner id"], Any[5, "abandoned yes or no"], Any[5, "breed code"], Any[5, "size code"], Any[5, "name"], Any[5, "age"], Any[5, "date of birth"], Any[5, "gender"], Any[5, "weight"], Any[5, "date arrived"], Any[5, "date adopted"], Any[5, "date departed"], Any[6, "professional id"], Any[6, "role code"], Any[6, "first name"], Any[6, "street"], Any[6, "city"], Any[6, "state"], Any[6, "zip code"], Any[6, "last name"], Any[6, "email address"], Any[6, "home phone"], Any[6, "cell number"], Any[7, "treatment id"], Any[7, "dog id"], Any[7, "professional id"], Any[7, "treatment type code"], Any[7, "date of treatment"], Any[7, "cost of treatment"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "breed code"], Any[0, "breed name"], Any[1, "charge id"], Any[1, "charge type"], Any[1, "charge amount"], Any[2, "size code"], Any[2, "size description"], Any[3, "treatment type code"], Any[3, "treatment type description"], Any[4, "owner id"], Any[4, "first name"], Any[4, "last name"], Any[4, "street"], Any[4, "city"], Any[4, "state"], Any[4, "zip code"], Any[4, "email address"], Any[4, "home phone"], Any[4, "cell number"], Any[5, "dog id"], Any[5, "owner id"], Any[5, "abandoned yes or no"], Any[5, "breed code"], Any[5, "size code"], Any[5, "name"], Any[5, "age"], Any[5, "date of birth"], Any[5, "gender"], Any[5, "weight"], Any[5, "date arrived"], Any[5, "date adopted"], Any[5, "date departed"], Any[6, "professional id"], Any[6, "role code"], Any[6, "first name"], Any[6, "street"], Any[6, "city"], Any[6, "state"], Any[6, "zip code"], Any[6, "last name"], Any[6, "email address"], Any[6, "home phone"], Any[6, "cell number"], Any[7, "treatment id"], Any[7, "dog id"], Any[7, "professional id"], Any[7, "treatment type code"], Any[7, "date of treatment"], Any[7, "cost of treatment"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["owner id", "owner id", "size code", "breed code", "dog id", "professional id", "treatment type code"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "breed name"], Any[1, "charge id"], Any[1, "charge type"], Any[1, "charge amount"], Any[2, "size description"], Any[3, "treatment type description"], Any[4, "first name"], Any[4, "last name"], Any[4, "street"], Any[4, "city"], Any[4, "state"], Any[4, "zip code"], Any[4, "email address"], Any[4, "home phone"], Any[4, "cell number"], Any[5, "abandoned yes or no"], Any[5, "name"], Any[5, "age"], Any[5, "date of birth"], Any[5, "gender"], Any[5, "weight"], Any[5, "date arrived"], Any[5, "date adopted"], Any[5, "date departed"], Any[6, "role code"], Any[6, "first name"], Any[6, "street"], Any[6, "city"], Any[6, "state"], Any[6, "zip code"], Any[6, "last name"], Any[6, "email address"], Any[6, "home phone"], Any[6, "cell number"], Any[7, "treatment id"], Any[7, "date of treatment"], Any[7, "cost of treatment"]]
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





PClean.@model DogKennelsModel begin
    @class Breeds begin
        breed_code ~ ChooseUniformly(possibilities[:breed_code])
        breed_name ~ ChooseUniformly(possibilities[:breed_name])
    end

    @class Charges begin
        charge_id ~ Unmodeled()
        charge_type ~ ChooseUniformly(possibilities[:charge_type])
        charge_amount ~ ChooseUniformly(possibilities[:charge_amount])
    end

    @class Sizes begin
        size_code ~ ChooseUniformly(possibilities[:size_code])
        size_description ~ ChooseUniformly(possibilities[:size_description])
    end

    @class Treatment_Types begin
        treatment_type_code ~ ChooseUniformly(possibilities[:treatment_type_code])
        treatment_type_description ~ ChooseUniformly(possibilities[:treatment_type_description])
    end

    @class Owners begin
        owner_id ~ Unmodeled()
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        street ~ ChooseUniformly(possibilities[:street])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        home_phone ~ ChooseUniformly(possibilities[:home_phone])
        cell_number ~ ChooseUniformly(possibilities[:cell_number])
    end

    @class Professionals begin
        professional_id ~ Unmodeled()
        role_code ~ ChooseUniformly(possibilities[:role_code])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        street ~ ChooseUniformly(possibilities[:street])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ ChooseUniformly(possibilities[:state])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        home_phone ~ ChooseUniformly(possibilities[:home_phone])
        cell_number ~ ChooseUniformly(possibilities[:cell_number])
    end

    @class Obs begin
        breeds ~ Breeds
        charges ~ Charges
        sizes ~ Sizes
        treatment_Types ~ Treatment_Types
        owners ~ Owners
        professionals ~ Professionals
        dog_id ~ Unmodeled()
        abandoned_yes_or_no ~ ChooseUniformly(possibilities[:abandoned_yes_or_no])
        name ~ ChooseUniformly(possibilities[:name])
        age ~ ChooseUniformly(possibilities[:age])
        date_of_birth ~ TimePrior(possibilities[:date_of_birth])
        gender ~ ChooseUniformly(possibilities[:gender])
        weight ~ ChooseUniformly(possibilities[:weight])
        date_arrived ~ TimePrior(possibilities[:date_arrived])
        date_adopted ~ TimePrior(possibilities[:date_adopted])
        date_departed ~ TimePrior(possibilities[:date_departed])
        treatment_id ~ Unmodeled()
        date_of_treatment ~ TimePrior(possibilities[:date_of_treatment])
        cost_of_treatment ~ ChooseUniformly(possibilities[:cost_of_treatment])
    end
end

query = @query DogKennelsModel.Obs [
    breeds_breed_code breeds.breed_code
    breeds_breed_name breeds.breed_name
    charges_charge_id charges.charge_id
    charges_charge_type charges.charge_type
    charges_charge_amount charges.charge_amount
    sizes_size_code sizes.size_code
    sizes_size_description sizes.size_description
    treatment_types_treatment_type_code treatment_Types.treatment_type_code
    treatment_types_treatment_type_description treatment_Types.treatment_type_description
    owners_owner_id owners.owner_id
    owners_first_name owners.first_name
    owners_last_name owners.last_name
    owners_street owners.street
    owners_city owners.city
    owners_state owners.state
    owners_zip_code owners.zip_code
    owners_email_address owners.email_address
    owners_home_phone owners.home_phone
    owners_cell_number owners.cell_number
    dogs_dog_id dog_id
    dogs_abandoned_yes_or_no abandoned_yes_or_no
    dogs_name name
    dogs_age age
    dogs_date_of_birth date_of_birth
    dogs_gender gender
    dogs_weight weight
    dogs_date_arrived date_arrived
    dogs_date_adopted date_adopted
    dogs_date_departed date_departed
    professionals_professional_id professionals.professional_id
    professionals_role_code professionals.role_code
    professionals_first_name professionals.first_name
    professionals_street professionals.street
    professionals_city professionals.city
    professionals_state professionals.state
    professionals_zip_code professionals.zip_code
    professionals_last_name professionals.last_name
    professionals_email_address professionals.email_address
    professionals_home_phone professionals.home_phone
    professionals_cell_number professionals.cell_number
    treatments_treatment_id treatment_id
    treatments_date_of_treatment date_of_treatment
    treatments_cost_of_treatment cost_of_treatment
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
