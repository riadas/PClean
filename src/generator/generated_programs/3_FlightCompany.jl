using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("airport_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("airport_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "city"], Any[0, "country"], Any[0, "iata"], Any[0, "icao"], Any[0, "name"], Any[1, "id"], Any[1, "name"], Any[1, "type"], Any[1, "principal activities"], Any[1, "incorporated in"], Any[1, "group equity shareholding"], Any[2, "id"], Any[2, "vehicle flight number"], Any[2, "date"], Any[2, "pilot"], Any[2, "velocity"], Any[2, "altitude"], Any[2, "airport id"], Any[2, "company id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "city"], Any[0, "country"], Any[0, "iata"], Any[0, "icao"], Any[0, "name"], Any[1, "id"], Any[1, "name"], Any[1, "type"], Any[1, "principal activities"], Any[1, "incorporated in"], Any[1, "group equity shareholding"], Any[2, "id"], Any[2, "vehicle flight number"], Any[2, "date"], Any[2, "pilot"], Any[2, "velocity"], Any[2, "altitude"], Any[2, "airport id"], Any[2, "company id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model FlightCompanyModel begin
    @class Airport begin
        id ~ Unmodeled()
        city ~ ChooseUniformly(possibilities[:city])
        country ~ ChooseUniformly(possibilities[:country])
        iata ~ ChooseUniformly(possibilities[:iata])
        icao ~ ChooseUniformly(possibilities[:icao])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Operate_Company begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        type ~ ChooseUniformly(possibilities[:type])
        principal_activities ~ ChooseUniformly(possibilities[:principal_activities])
        incorporated_in ~ ChooseUniformly(possibilities[:incorporated_in])
        group_equity_shareholding ~ ChooseUniformly(possibilities[:group_equity_shareholding])
    end

    @class Flight begin
        id ~ Unmodeled()
        vehicle_flight_number ~ ChooseUniformly(possibilities[:vehicle_flight_number])
        date ~ ChooseUniformly(possibilities[:date])
        pilot ~ ChooseUniformly(possibilities[:pilot])
        velocity ~ ChooseUniformly(possibilities[:velocity])
        altitude ~ ChooseUniformly(possibilities[:altitude])
        airport_id ~ ChooseUniformly(possibilities[:airport_id])
        company_id ~ ChooseUniformly(possibilities[:company_id])
    end

    @class Obs begin
        airport ~ Airport
        operate_Company ~ Operate_Company
        flight ~ Flight
    end
end

query = @query FlightCompanyModel.Obs [
    airport_id airport.id
    airport_city airport.city
    airport_country airport.country
    airport_iata airport.iata
    airport_icao airport.icao
    airport_name airport.name
    operate_company_id operate_Company.id
    operate_company_name operate_Company.name
    operate_company_type operate_Company.type
    operate_company_principal_activities operate_Company.principal_activities
    operate_company_incorporated_in operate_Company.incorporated_in
    operate_company_group_equity_shareholding operate_Company.group_equity_shareholding
    flight_id flight.id
    vehicle_flight_number flight.vehicle_flight_number
    flight_date flight.date
    flight_pilot flight.pilot
    flight_velocity flight.velocity
    flight_altitude flight.altitude
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
