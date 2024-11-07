using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("flight_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("flight_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "flight number"], Any[0, "origin"], Any[0, "destination"], Any[0, "distance"], Any[0, "departure date"], Any[0, "arrival date"], Any[0, "price"], Any[0, "airline id"], Any[1, "airline id"], Any[1, "name"], Any[1, "distance"], Any[2, "employee id"], Any[2, "name"], Any[2, "salary"], Any[3, "employee id"], Any[3, "airline id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "flight number"], Any[0, "origin"], Any[0, "destination"], Any[0, "distance"], Any[0, "departure date"], Any[0, "arrival date"], Any[0, "price"], Any[0, "airline id"], Any[1, "airline id"], Any[1, "name"], Any[1, "distance"], Any[2, "employee id"], Any[2, "name"], Any[2, "salary"], Any[3, "employee id"], Any[3, "airline id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Flight1Model begin
    @class Flight begin
        flight_number ~ ChooseUniformly(possibilities[:flight_number])
        origin ~ ChooseUniformly(possibilities[:origin])
        destination ~ ChooseUniformly(possibilities[:destination])
        distance ~ ChooseUniformly(possibilities[:distance])
        departure_date ~ TimePrior(possibilities[:departure_date])
        arrival_date ~ TimePrior(possibilities[:arrival_date])
        price ~ ChooseUniformly(possibilities[:price])
        airline_id ~ ChooseUniformly(possibilities[:airline_id])
    end

    @class Aircraft begin
        airline_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        distance ~ ChooseUniformly(possibilities[:distance])
    end

    @class Employee begin
        employee_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        salary ~ ChooseUniformly(possibilities[:salary])
    end

    @class Certificate begin
        employee_id ~ Unmodeled()
        airline_id ~ ChooseUniformly(possibilities[:airline_id])
    end

    @class Obs begin
        flight ~ Flight
        aircraft ~ Aircraft
        employee ~ Employee
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
    aircraft_airline_id aircraft.airline_id
    aircraft_name aircraft.name
    aircraft_distance aircraft.distance
    employee_id employee.employee_id
    employee_name employee.name
    employee_salary employee.salary
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
