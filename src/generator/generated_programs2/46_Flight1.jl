using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("flight_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("flight_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "flight number"], Any[0, "origin"], Any[0, "destination"], Any[0, "distance"], Any[0, "departure date"], Any[0, "arrival date"], Any[0, "price"], Any[0, "airline id"], Any[1, "airline id"], Any[1, "name"], Any[1, "distance"], Any[2, "employee id"], Any[2, "name"], Any[2, "salary"], Any[3, "employee id"], Any[3, "airline id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "flight number"], Any[0, "origin"], Any[0, "destination"], Any[0, "distance"], Any[0, "departure date"], Any[0, "arrival date"], Any[0, "price"], Any[0, "airline id"], Any[1, "airline id"], Any[1, "name"], Any[1, "distance"], Any[2, "employee id"], Any[2, "name"], Any[2, "salary"], Any[3, "employee id"], Any[3, "airline id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "flight number"], Any[0, "origin"], Any[0, "destination"], Any[0, "distance"], Any[0, "departure date"], Any[0, "arrival date"], Any[0, "price"], Any[0, "airline id"], Any[1, "airline id"], Any[1, "name"], Any[1, "distance"], Any[2, "employee id"], Any[2, "name"], Any[2, "salary"], Any[3, "employee id"], Any[3, "airline id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "flight number"], Any[0, "origin"], Any[0, "destination"], Any[0, "distance"], Any[0, "departure date"], Any[0, "arrival date"], Any[0, "price"], Any[0, "airline id"], Any[1, "airline id"], Any[1, "name"], Any[1, "distance"], Any[2, "employee id"], Any[2, "name"], Any[2, "salary"], Any[3, "employee id"], Any[3, "airline id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "flight number"], Any[0, "origin"], Any[0, "destination"], Any[0, "distance"], Any[0, "departure date"], Any[0, "arrival date"], Any[0, "price"], Any[0, "airline id"], Any[1, "airline id"], Any[1, "name"], Any[1, "distance"], Any[2, "employee id"], Any[2, "name"], Any[2, "salary"], Any[3, "employee id"], Any[3, "airline id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[8, 9], Any[16, 9], Any[15, 12]])
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







PClean.@model Flight1Model begin
    @class Aircraft begin
        name ~ ChooseUniformly(possibilities[:name])
        distance ~ ChooseUniformly(possibilities[:distance])
    end

    @class Employee begin
        name ~ ChooseUniformly(possibilities[:name])
        salary ~ ChooseUniformly(possibilities[:salary])
    end

    @class Certificate begin
        aircraft ~ Aircraft
    end

    @class Flight begin
        flight_number ~ ChooseUniformly(possibilities[:flight_number])
        origin ~ ChooseUniformly(possibilities[:origin])
        destination ~ ChooseUniformly(possibilities[:destination])
        distance ~ ChooseUniformly(possibilities[:distance])
        departure_date ~ TimePrior(possibilities[:departure_date])
        arrival_date ~ TimePrior(possibilities[:arrival_date])
        price ~ ChooseUniformly(possibilities[:price])
        aircraft ~ Aircraft
    end

    @class Obs begin
        flight ~ Flight
        certificate ~ Certificate
    end
end

query = @query Flight1Model.Obs [
    flight_number flight.flight_number
    flight_origin flight.origin
    flight_destination flight.destination
    flight_distance flight.distance
    flight_departure_date flight.departure_date
    flight_arrival_date flight.arrival_date
    flight_price flight.price
    aircraft_airline_id flight.aircraft.airline_id
    aircraft_name flight.aircraft.name
    aircraft_distance flight.aircraft.distance
    employee_id certificate.employee.employee_id
    employee_name certificate.employee.name
    employee_salary certificate.employee.salary
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
