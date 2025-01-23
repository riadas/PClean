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
if length(names(dirty_table)) != length(Any[Any[0, "ProviderNumber"], Any[0, "HospitalName"], Any[0, "Address1"], Any[0, "City"], Any[0, "State"], Any[0, "ZipCode"], Any[0, "CountyName"], Any[0, "PhoneNumber"], Any[0, "HospitalType"], Any[0, "HospitalOwner"], Any[0, "EmergencyService"], Any[0, "Condition"], Any[0, "MeasureCode"], Any[0, "MeasureName"], Any[0, "Stateavg"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[0, "ProviderNumber"], Any[0, "HospitalName"], Any[0, "Address1"], Any[0, "City"], Any[0, "State"], Any[0, "ZipCode"], Any[0, "CountyName"], Any[0, "PhoneNumber"], Any[0, "HospitalType"], Any[0, "HospitalOwner"], Any[0, "EmergencyService"], Any[0, "Condition"], Any[0, "MeasureCode"], Any[0, "MeasureName"], Any[0, "Stateavg"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = Any[]
column_names_without_foreign_keys = Any[Any[0, "ProviderNumber"], Any[0, "HospitalName"], Any[0, "Address1"], Any[0, "City"], Any[0, "State"], Any[0, "ZipCode"], Any[0, "CountyName"], Any[0, "PhoneNumber"], Any[0, "HospitalType"], Any[0, "HospitalOwner"], Any[0, "EmergencyService"], Any[0, "Condition"], Any[0, "MeasureCode"], Any[0, "MeasureName"], Any[0, "Stateavg"]]
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







PClean.@model HospitalDataModel begin
    @class Hospital_measures begin
        ProviderNumber ~ ChooseUniformly(possibilities[:ProviderNumber])
        HospitalName ~ ChooseUniformly(possibilities[:HospitalName])
        Address1 ~ ChooseUniformly(possibilities[:Address1])
        City ~ ChooseUniformly(possibilities[:City])
        State ~ ChooseUniformly(possibilities[:State])
        ZipCode ~ ChooseUniformly(possibilities[:ZipCode])
        CountyName ~ StringPrior(3, 10, possibilities[:CountyName])
        PhoneNumber ~ StringPrior(10, 10, possibilities[:PhoneNumber])
        HospitalType ~ ChooseUniformly(possibilities[:HospitalType])
        HospitalOwner ~ ChooseUniformly(possibilities[:HospitalOwner])
        EmergencyService ~ StringPrior(2, 3, possibilities[:EmergencyService])
        Condition ~ ChooseUniformly(possibilities[:Condition])
        MeasureCode ~ StringPrior(4, 11, possibilities[:MeasureCode])
        MeasureName ~ ChooseUniformly(possibilities[:MeasureName])
        Stateavg ~ StringPrior(7, 14, possibilities[:Stateavg])
    end

    @class Obs begin
        hospital_measures ~ Hospital_measures
        ProviderNumber ~ AddTypos(hospital_measures.ProviderNumber, 2)
        HospitalName ~ AddTypos(hospital_measures.HospitalName, 2)
        Address1 ~ AddTypos(hospital_measures.Address1, 2)
        City ~ AddTypos(hospital_measures.City, 2)
        State ~ AddTypos(hospital_measures.State, 2)
        ZipCode ~ AddTypos(hospital_measures.ZipCode, 2)
        CountyName ~ AddTypos(hospital_measures.CountyName, 2)
        PhoneNumber ~ AddTypos(hospital_measures.PhoneNumber, 2)
        HospitalType ~ AddTypos(hospital_measures.HospitalType, 2)
        HospitalOwner ~ AddTypos(hospital_measures.HospitalOwner, 2)
        EmergencyService ~ AddTypos(hospital_measures.EmergencyService, 2)
        Condition ~ AddTypos(hospital_measures.Condition, 2)
        MeasureCode ~ AddTypos(hospital_measures.MeasureCode, 2)
        MeasureName ~ AddTypos(hospital_measures.MeasureName, 2)
        Stateavg ~ AddTypos(hospital_measures.Stateavg, 2)
    end
end

query = @query HospitalDataModel.Obs [
    ProviderNumber hospital_measures.ProviderNumber ProviderNumber
    HospitalName hospital_measures.HospitalName HospitalName
    Address1 hospital_measures.Address1 Address1
    City hospital_measures.City City
    State hospital_measures.State State
    ZipCode hospital_measures.ZipCode ZipCode
    CountyName hospital_measures.CountyName CountyName
    PhoneNumber hospital_measures.PhoneNumber PhoneNumber
    HospitalType hospital_measures.HospitalType HospitalType
    HospitalOwner hospital_measures.HospitalOwner HospitalOwner
    EmergencyService hospital_measures.EmergencyService EmergencyService
    Condition hospital_measures.Condition Condition
    MeasureCode hospital_measures.MeasureCode MeasureCode
    MeasureName hospital_measures.MeasureName MeasureName
    Stateavg hospital_measures.Stateavg Stateavg
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
