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
if length(names(dirty_table)) != length(Any[Any[0, "provider_number"], Any[0, "hospital_name"], Any[0, "address1"], Any[0, "phone_number"], Any[0, "hospital_type"], Any[0, "hospital_owner"], Any[0, "emergency_service"], Any[0, "city"], Any[0, "state"], Any[0, "zip_code"], Any[0, "county_name"], Any[1, "condition"], Any[1, "measure_code"], Any[1, "measure_name"], Any[1, "state_avg"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[0, "provider_number"], Any[0, "hospital_name"], Any[0, "address1"], Any[0, "phone_number"], Any[0, "hospital_type"], Any[0, "hospital_owner"], Any[0, "emergency_service"], Any[0, "city"], Any[0, "state"], Any[0, "zip_code"], Any[0, "county_name"], Any[1, "condition"], Any[1, "measure_code"], Any[1, "measure_name"], Any[1, "state_avg"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[0, "provider_number"], Any[0, "hospital_name"], Any[0, "address1"], Any[0, "phone_number"], Any[0, "hospital_type"], Any[0, "hospital_owner"], Any[0, "emergency_service"], Any[0, "city"], Any[0, "state"], Any[0, "zip_code"], Any[0, "county_name"], Any[1, "condition"], Any[1, "measure_code"], Any[1, "measure_name"], Any[1, "state_avg"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[])
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







PClean.@model HospitalQualityModel begin
    @class Hospital begin
        provider_number ~ ChooseUniformly(possibilities[:provider_number])
        hospital_name ~ ChooseUniformly(possibilities[:hospital_name])
        address1 ~ StringPrior(10, 28, possibilities[:address1])
        phone_number ~ StringPrior(10, 10, possibilities[:phone_number])
        hospital_type ~ ChooseUniformly(possibilities[:hospital_type])
        hospital_owner ~ ChooseUniformly(possibilities[:hospital_owner])
        emergency_service ~ StringPrior(2, 3, possibilities[:emergency_service])
        city ~ ChooseUniformly(possibilities[:city])
        state ~ StringPrior(2, 2, possibilities[:state])
        zip_code ~ ChooseUniformly(possibilities[:zip_code])
        county_name ~ StringPrior(3, 10, possibilities[:county_name])
    end

    @class Measure begin
        condition ~ ChooseUniformly(possibilities[:condition])
        measure_code ~ ChooseUniformly(possibilities[:measure_code])
        measure_name ~ StringPrior(46, 184, possibilities[:measure_name])
        state_avg ~ StringPrior(7, 14, possibilities[:state_avg])
    end

    @class Obs begin
        hospital ~ Hospital
        measure ~ Measure
        provider_number ~ AddTypos(hospital.provider_number, 2)
        hospital_name ~ AddTypos(hospital.hospital_name, 2)
        address1 ~ AddTypos(hospital.address1, 2)
        phone_number ~ AddTypos(hospital.phone_number, 2)
        hospital_type ~ AddTypos(hospital.hospital_type, 2)
        hospital_owner ~ AddTypos(hospital.hospital_owner, 2)
        emergency_service ~ AddTypos(hospital.emergency_service, 2)
        city ~ AddTypos(hospital.city, 2)
        state ~ AddTypos(hospital.state, 2)
        zip_code ~ AddTypos(hospital.zip_code, 2)
        county_name ~ AddTypos(hospital.county_name, 2)
        condition ~ AddTypos(measure.condition, 2)
        measure_code ~ AddTypos(measure.measure_code, 2)
        measure_name ~ AddTypos(measure.measure_name, 2)
        state_avg ~ AddTypos(measure.state_avg, 2)
    end
end

query = @query HospitalQualityModel.Obs [
    ProviderNumber hospital.provider_number provider_number
    HospitalName hospital.hospital_name hospital_name
    Address1 hospital.address1 address1
    PhoneNumber hospital.phone_number phone_number
    HospitalType hospital.hospital_type hospital_type
    HospitalOwner hospital.hospital_owner hospital_owner
    EmergencyService hospital.emergency_service emergency_service
    City hospital.city city
    State hospital.state state
    ZipCode hospital.zip_code zip_code
    CountyName hospital.county_name county_name
    Condition measure.condition condition
    MeasureCode measure.measure_code measure_code
    MeasureName measure.measure_name measure_name
    Stateavg measure.state_avg state_avg
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
