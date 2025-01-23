using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("datasets/hospital_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("datasets/hospital_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame
clean_table[!, :PhoneNumber] = map(x -> "$x", clean_table[!, :PhoneNumber])
clean_table[!, :ZipCode] = map(x -> "$x", clean_table[!, :ZipCode])
clean_table[!, :ProviderNumber] = map(x -> "$x", clean_table[!, :ProviderNumber])

subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[0, "provider_number"], Any[0, "hospital_name"], Any[0, "address1"], Any[0, "city"], Any[0, "state"], Any[0, "zip_code"], Any[0, "county_name"], Any[0, "phone_number"], Any[0, "hospital_type"], Any[0, "hospital_owner"], Any[0, "emergency_service"], Any[1, "measure_code"], Any[1, "measure_name"], Any[1, "condition"], Any[2, "hospital_id"], Any[2, "measure_id"], Any[2, "state_avg"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[0, "provider_number"], Any[0, "hospital_name"], Any[0, "address1"], Any[0, "city"], Any[0, "state"], Any[0, "zip_code"], Any[0, "county_name"], Any[0, "phone_number"], Any[0, "hospital_type"], Any[0, "hospital_owner"], Any[0, "emergency_service"], Any[1, "measure_code"], Any[1, "measure_name"], Any[1, "condition"], Any[2, "hospital_id"], Any[2, "measure_id"], Any[2, "state_avg"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["hospital_id", "measure_id"]
column_names_without_foreign_keys = Any[Any[0, "provider_number"], Any[0, "hospital_name"], Any[0, "address1"], Any[0, "city"], Any[0, "state"], Any[0, "zip_code"], Any[0, "county_name"], Any[0, "phone_number"], Any[0, "hospital_type"], Any[0, "hospital_owner"], Any[0, "emergency_service"], Any[1, "measure_code"], Any[1, "measure_name"], Any[1, "condition"], Any[2, "state_avg"]]
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







PClean.@model HospitalQualityModel begin
    @class Hospitals begin
        provider_number ~ StringPrior(5, 5, possibilities[:provider_number])
        hospital_name ~ ChooseUniformly(possibilities[:hospital_name])
        address1 ~ ChooseUniformly(possibilities[:address1])
        city ~ StringPrior(3, 12, possibilities[:city])
        state ~ StringPrior(2, 2, possibilities[:state])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
        county_name ~ StringPrior(3, 10, possibilities[:county_name])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
        hospital_type ~ ChooseUniformly(possibilities[:hospital_type])
        hospital_owner ~ ChooseUniformly(possibilities[:hospital_owner])
        emergency_service ~ ChooseUniformly(possibilities[:emergency_service])
    end

    @class Measures begin
        measure_code ~ ChooseUniformly(possibilities[:measure_code])
        measure_name ~ ChooseUniformly(possibilities[:measure_name])
        condition ~ ChooseUniformly(possibilities[:condition])
    end

    @class Obs begin
        hospitals ~ Hospitals
        measures ~ Measures
        provider_number ~ AddTypos(hospitals.provider_number, 2)
        hospital_name ~ AddTypos(hospitals.hospital_name, 2)
        address1 ~ AddTypos(hospitals.address1, 2)
        city ~ AddTypos(hospitals.city, 2)
        state ~ AddTypos(hospitals.state, 2)
        zip_code ~ AddTypos(hospitals.zip_code, 2)
        county_name ~ AddTypos(hospitals.county_name, 2)
        phone_number ~ AddTypos(hospitals.phone_number, 2)
        hospital_type ~ AddTypos(hospitals.hospital_type, 2)
        hospital_owner ~ AddTypos(hospitals.hospital_owner, 2)
        emergency_service ~ AddTypos(hospitals.emergency_service, 2)
        measure_code ~ AddTypos(measures.measure_code, 2)
        measure_name ~ AddTypos(measures.measure_name, 2)
        condition ~ AddTypos(measures.condition, 2)
        state_avg ~ ChooseUniformly(possibilities[:state_avg])
        state_avg_typo ~ AddTypos(state_avg, 2)
    end
end

query = @query HospitalQualityModel.Obs [
    ProviderNumber hospitals.provider_number provider_number
    HospitalName hospitals.hospital_name hospital_name
    Address1 hospitals.address1 address1
    City hospitals.city city
    State hospitals.state state
    ZipCode hospitals.zip_code zip_code
    CountyName hospitals.county_name county_name
    PhoneNumber hospitals.phone_number phone_number
    HospitalType hospitals.hospital_type hospital_type
    HospitalOwner hospitals.hospital_owner hospital_owner
    EmergencyService hospitals.emergency_service emergency_service
    MeasureCode measures.measure_code measure_code
    MeasureName measures.measure_name measure_name
    Condition measures.condition condition
    Stateavg state_avg state_avg_typo
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
