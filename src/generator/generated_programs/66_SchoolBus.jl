using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("driver_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("driver_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "driver id"], Any[0, "name"], Any[0, "party"], Any[0, "home city"], Any[0, "age"], Any[1, "school id"], Any[1, "grade"], Any[1, "school"], Any[1, "location"], Any[1, "type"], Any[2, "school id"], Any[2, "driver id"], Any[2, "years working"], Any[2, "if full time"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "driver id"], Any[0, "name"], Any[0, "party"], Any[0, "home city"], Any[0, "age"], Any[1, "school id"], Any[1, "grade"], Any[1, "school"], Any[1, "location"], Any[1, "type"], Any[2, "school id"], Any[2, "driver id"], Any[2, "years working"], Any[2, "if full time"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model SchoolBusModel begin
    @class Driver begin
        driver_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        party ~ ChooseUniformly(possibilities[:party])
        home_city ~ ChooseUniformly(possibilities[:home_city])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class School begin
        school_id ~ Unmodeled()
        grade ~ ChooseUniformly(possibilities[:grade])
        school ~ ChooseUniformly(possibilities[:school])
        location ~ ChooseUniformly(possibilities[:location])
        type ~ ChooseUniformly(possibilities[:type])
    end

    @class School_Bus begin
        school_id ~ Unmodeled()
        driver_id ~ ChooseUniformly(possibilities[:driver_id])
        years_working ~ ChooseUniformly(possibilities[:years_working])
        if_full_time ~ ChooseUniformly(possibilities[:if_full_time])
    end

    @class Obs begin
        driver ~ Driver
        school ~ School
        school_Bus ~ School_Bus
    end
end

query = @query SchoolBusModel.Obs [
    driver_id driver.driver_id
    driver_name driver.name
    driver_party driver.party
    driver_home_city driver.home_city
    driver_age driver.age
    school_id school.school_id
    school_grade school.grade
    school school.school
    school_location school.location
    school_type school.type
    school_bus_years_working school_Bus.years_working
    school_bus_if_full_time school_Bus.if_full_time
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
