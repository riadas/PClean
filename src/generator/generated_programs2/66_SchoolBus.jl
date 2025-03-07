using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("driver_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("driver_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "driver id"], Any[0, "name"], Any[0, "party"], Any[0, "home city"], Any[0, "age"], Any[1, "school id"], Any[1, "grade"], Any[1, "school"], Any[1, "location"], Any[1, "type"], Any[2, "school id"], Any[2, "driver id"], Any[2, "years working"], Any[2, "if full time"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "driver id"], Any[0, "name"], Any[0, "party"], Any[0, "home city"], Any[0, "age"], Any[1, "school id"], Any[1, "grade"], Any[1, "school"], Any[1, "location"], Any[1, "type"], Any[2, "school id"], Any[2, "driver id"], Any[2, "years working"], Any[2, "if full time"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "driver id"], Any[0, "name"], Any[0, "party"], Any[0, "home city"], Any[0, "age"], Any[1, "school id"], Any[1, "grade"], Any[1, "school"], Any[1, "location"], Any[1, "type"], Any[2, "school id"], Any[2, "driver id"], Any[2, "years working"], Any[2, "if full time"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "driver id"], Any[0, "name"], Any[0, "party"], Any[0, "home city"], Any[0, "age"], Any[1, "school id"], Any[1, "grade"], Any[1, "school"], Any[1, "location"], Any[1, "type"], Any[2, "school id"], Any[2, "driver id"], Any[2, "years working"], Any[2, "if full time"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "driver id"], Any[0, "name"], Any[0, "party"], Any[0, "home city"], Any[0, "age"], Any[1, "school id"], Any[1, "grade"], Any[1, "school"], Any[1, "location"], Any[1, "type"], Any[2, "school id"], Any[2, "driver id"], Any[2, "years working"], Any[2, "if full time"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[12, 1], Any[11, 6]])
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







PClean.@model SchoolBusModel begin
    @class Driver begin
        name ~ ChooseUniformly(possibilities[:name])
        party ~ ChooseUniformly(possibilities[:party])
        home_city ~ ChooseUniformly(possibilities[:home_city])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class School begin
        grade ~ ChooseUniformly(possibilities[:grade])
        school ~ ChooseUniformly(possibilities[:school])
        location ~ ChooseUniformly(possibilities[:location])
        type ~ ChooseUniformly(possibilities[:type])
    end

    @class School_bus begin
        driver ~ Driver
        years_working ~ ChooseUniformly(possibilities[:years_working])
        if_full_time ~ ChooseUniformly(possibilities[:if_full_time])
    end

    @class Obs begin
        school_bus ~ School_bus
    end
end

query = @query SchoolBusModel.Obs [
    driver_id school_bus.driver.driver_id
    driver_name school_bus.driver.name
    driver_party school_bus.driver.party
    driver_home_city school_bus.driver.home_city
    driver_age school_bus.driver.age
    school_id school_bus.school.school_id
    school_grade school_bus.school.grade
    school school_bus.school.school
    school_location school_bus.school.location
    school_type school_bus.school.type
    school_bus_years_working school_bus.years_working
    school_bus_if_full_time school_bus.if_full_time
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
