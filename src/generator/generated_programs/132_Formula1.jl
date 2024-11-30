using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("circuits_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("circuits_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "circuit id"], Any[0, "circuit reference"], Any[0, "name"], Any[0, "location"], Any[0, "country"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "altitude"], Any[0, "url"], Any[1, "race id"], Any[1, "year"], Any[1, "round"], Any[1, "circuit id"], Any[1, "name"], Any[1, "date"], Any[1, "time"], Any[1, "url"], Any[2, "driver id"], Any[2, "driver reference"], Any[2, "number"], Any[2, "code"], Any[2, "forename"], Any[2, "surname"], Any[2, "dob"], Any[2, "nationality"], Any[2, "url"], Any[3, "status id"], Any[3, "status"], Any[4, "year"], Any[4, "url"], Any[5, "constructor id"], Any[5, "constructor reference"], Any[5, "name"], Any[5, "nationality"], Any[5, "url"], Any[6, "constructor standings id"], Any[6, "race id"], Any[6, "constructor id"], Any[6, "points"], Any[6, "position"], Any[6, "position text"], Any[6, "wins"], Any[7, "result id"], Any[7, "race id"], Any[7, "driver id"], Any[7, "constructor id"], Any[7, "number"], Any[7, "grid"], Any[7, "position"], Any[7, "position text"], Any[7, "position order"], Any[7, "points"], Any[7, "laps"], Any[7, "time"], Any[7, "milliseconds"], Any[7, "fastest lap"], Any[7, "rank"], Any[7, "fastest lap time"], Any[7, "fastest lap speed"], Any[7, "status id"], Any[8, "driver standings id"], Any[8, "race id"], Any[8, "driver id"], Any[8, "points"], Any[8, "position"], Any[8, "position text"], Any[8, "wins"], Any[9, "constructor results id"], Any[9, "race id"], Any[9, "constructor id"], Any[9, "points"], Any[9, "status"], Any[10, "qualify id"], Any[10, "race id"], Any[10, "driver id"], Any[10, "constructor id"], Any[10, "number"], Any[10, "position"], Any[10, "q1"], Any[10, "q2"], Any[10, "q3"], Any[11, "race id"], Any[11, "driver id"], Any[11, "stop"], Any[11, "lap"], Any[11, "time"], Any[11, "duration"], Any[11, "milliseconds"], Any[12, "race id"], Any[12, "driver id"], Any[12, "lap"], Any[12, "position"], Any[12, "time"], Any[12, "milliseconds"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "circuit id"], Any[0, "circuit reference"], Any[0, "name"], Any[0, "location"], Any[0, "country"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "altitude"], Any[0, "url"], Any[1, "race id"], Any[1, "year"], Any[1, "round"], Any[1, "circuit id"], Any[1, "name"], Any[1, "date"], Any[1, "time"], Any[1, "url"], Any[2, "driver id"], Any[2, "driver reference"], Any[2, "number"], Any[2, "code"], Any[2, "forename"], Any[2, "surname"], Any[2, "dob"], Any[2, "nationality"], Any[2, "url"], Any[3, "status id"], Any[3, "status"], Any[4, "year"], Any[4, "url"], Any[5, "constructor id"], Any[5, "constructor reference"], Any[5, "name"], Any[5, "nationality"], Any[5, "url"], Any[6, "constructor standings id"], Any[6, "race id"], Any[6, "constructor id"], Any[6, "points"], Any[6, "position"], Any[6, "position text"], Any[6, "wins"], Any[7, "result id"], Any[7, "race id"], Any[7, "driver id"], Any[7, "constructor id"], Any[7, "number"], Any[7, "grid"], Any[7, "position"], Any[7, "position text"], Any[7, "position order"], Any[7, "points"], Any[7, "laps"], Any[7, "time"], Any[7, "milliseconds"], Any[7, "fastest lap"], Any[7, "rank"], Any[7, "fastest lap time"], Any[7, "fastest lap speed"], Any[7, "status id"], Any[8, "driver standings id"], Any[8, "race id"], Any[8, "driver id"], Any[8, "points"], Any[8, "position"], Any[8, "position text"], Any[8, "wins"], Any[9, "constructor results id"], Any[9, "race id"], Any[9, "constructor id"], Any[9, "points"], Any[9, "status"], Any[10, "qualify id"], Any[10, "race id"], Any[10, "driver id"], Any[10, "constructor id"], Any[10, "number"], Any[10, "position"], Any[10, "q1"], Any[10, "q2"], Any[10, "q3"], Any[11, "race id"], Any[11, "driver id"], Any[11, "stop"], Any[11, "lap"], Any[11, "time"], Any[11, "duration"], Any[11, "milliseconds"], Any[12, "race id"], Any[12, "driver id"], Any[12, "lap"], Any[12, "position"], Any[12, "time"], Any[12, "milliseconds"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["circuit id", "race id", "constructor id", "driver id", "race id", "constructor id", "driver id", "race id", "race id", "constructor id", "driver id", "race id", "constructor id", "driver id", "race id", "driver id", "race id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "circuit reference"], Any[0, "name"], Any[0, "location"], Any[0, "country"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "altitude"], Any[0, "url"], Any[1, "year"], Any[1, "round"], Any[1, "name"], Any[1, "date"], Any[1, "time"], Any[1, "url"], Any[2, "driver reference"], Any[2, "number"], Any[2, "code"], Any[2, "forename"], Any[2, "surname"], Any[2, "dob"], Any[2, "nationality"], Any[2, "url"], Any[3, "status id"], Any[3, "status"], Any[4, "year"], Any[4, "url"], Any[5, "constructor reference"], Any[5, "name"], Any[5, "nationality"], Any[5, "url"], Any[6, "constructor standings id"], Any[6, "points"], Any[6, "position"], Any[6, "position text"], Any[6, "wins"], Any[7, "result id"], Any[7, "number"], Any[7, "grid"], Any[7, "position"], Any[7, "position text"], Any[7, "position order"], Any[7, "points"], Any[7, "laps"], Any[7, "time"], Any[7, "milliseconds"], Any[7, "fastest lap"], Any[7, "rank"], Any[7, "fastest lap time"], Any[7, "fastest lap speed"], Any[7, "status id"], Any[8, "driver standings id"], Any[8, "points"], Any[8, "position"], Any[8, "position text"], Any[8, "wins"], Any[9, "constructor results id"], Any[9, "points"], Any[9, "status"], Any[10, "qualify id"], Any[10, "number"], Any[10, "position"], Any[10, "q1"], Any[10, "q2"], Any[10, "q3"], Any[11, "stop"], Any[11, "lap"], Any[11, "time"], Any[11, "duration"], Any[11, "milliseconds"], Any[12, "lap"], Any[12, "position"], Any[12, "time"], Any[12, "milliseconds"]]
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





PClean.@model Formula1Model begin
    @class Circuits begin
        circuit_id ~ Unmodeled()
        circuit_reference ~ ChooseUniformly(possibilities[:circuit_reference])
        name ~ ChooseUniformly(possibilities[:name])
        location ~ ChooseUniformly(possibilities[:location])
        country ~ ChooseUniformly(possibilities[:country])
        latitude ~ ChooseUniformly(possibilities[:latitude])
        longitude ~ ChooseUniformly(possibilities[:longitude])
        altitude ~ ChooseUniformly(possibilities[:altitude])
        url ~ ChooseUniformly(possibilities[:url])
    end

    @class Drivers begin
        driver_id ~ Unmodeled()
        driver_reference ~ ChooseUniformly(possibilities[:driver_reference])
        number ~ ChooseUniformly(possibilities[:number])
        code ~ ChooseUniformly(possibilities[:code])
        forename ~ ChooseUniformly(possibilities[:forename])
        surname ~ ChooseUniformly(possibilities[:surname])
        dob ~ ChooseUniformly(possibilities[:dob])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        url ~ ChooseUniformly(possibilities[:url])
    end

    @class Status begin
        status_id ~ Unmodeled()
        status ~ ChooseUniformly(possibilities[:status])
    end

    @class Seasons begin
        year ~ ChooseUniformly(possibilities[:year])
        url ~ ChooseUniformly(possibilities[:url])
    end

    @class Constructors begin
        constructor_id ~ Unmodeled()
        constructor_reference ~ ChooseUniformly(possibilities[:constructor_reference])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        url ~ ChooseUniformly(possibilities[:url])
    end

    @class Obs begin
        circuits ~ Circuits
        drivers ~ Drivers
        status ~ Status
        seasons ~ Seasons
        constructors ~ Constructors
        race_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        round ~ ChooseUniformly(possibilities[:round])
        name ~ ChooseUniformly(possibilities[:name])
        date ~ ChooseUniformly(possibilities[:date])
        time ~ ChooseUniformly(possibilities[:time])
        url ~ ChooseUniformly(possibilities[:url])
        constructor_standings_id ~ Unmodeled()
        points ~ ChooseUniformly(possibilities[:points])
        position ~ ChooseUniformly(possibilities[:position])
        position_text ~ ChooseUniformly(possibilities[:position_text])
        wins ~ ChooseUniformly(possibilities[:wins])
        result_id ~ Unmodeled()
        number ~ ChooseUniformly(possibilities[:number])
        grid ~ ChooseUniformly(possibilities[:grid])
        position ~ ChooseUniformly(possibilities[:position])
        position_text ~ ChooseUniformly(possibilities[:position_text])
        position_order ~ ChooseUniformly(possibilities[:position_order])
        points ~ ChooseUniformly(possibilities[:points])
        laps ~ ChooseUniformly(possibilities[:laps])
        time ~ ChooseUniformly(possibilities[:time])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
        fastest_lap ~ ChooseUniformly(possibilities[:fastest_lap])
        rank ~ ChooseUniformly(possibilities[:rank])
        fastest_lap_time ~ ChooseUniformly(possibilities[:fastest_lap_time])
        fastest_lap_speed ~ ChooseUniformly(possibilities[:fastest_lap_speed])
        status_id ~ ChooseUniformly(possibilities[:status_id])
        driver_standings_id ~ Unmodeled()
        points ~ ChooseUniformly(possibilities[:points])
        position ~ ChooseUniformly(possibilities[:position])
        position_text ~ ChooseUniformly(possibilities[:position_text])
        wins ~ ChooseUniformly(possibilities[:wins])
        constructor_results_id ~ Unmodeled()
        points ~ ChooseUniformly(possibilities[:points])
        status ~ ChooseUniformly(possibilities[:status])
        qualify_id ~ Unmodeled()
        number ~ ChooseUniformly(possibilities[:number])
        position ~ ChooseUniformly(possibilities[:position])
        q1 ~ ChooseUniformly(possibilities[:q1])
        q2 ~ ChooseUniformly(possibilities[:q2])
        q3 ~ ChooseUniformly(possibilities[:q3])
        stop ~ ChooseUniformly(possibilities[:stop])
        lap ~ ChooseUniformly(possibilities[:lap])
        time ~ ChooseUniformly(possibilities[:time])
        duration ~ ChooseUniformly(possibilities[:duration])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
        lap ~ ChooseUniformly(possibilities[:lap])
        position ~ ChooseUniformly(possibilities[:position])
        time ~ ChooseUniformly(possibilities[:time])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
    end
end

query = @query Formula1Model.Obs [
    circuits_circuit_id circuits.circuit_id
    circuits_circuit_reference circuits.circuit_reference
    circuits_name circuits.name
    circuits_location circuits.location
    circuits_country circuits.country
    circuits_latitude circuits.latitude
    circuits_longitude circuits.longitude
    circuits_altitude circuits.altitude
    circuits_url circuits.url
    races_race_id race_id
    races_year year
    races_round round
    races_name name
    races_date date
    races_time time
    races_url url
    drivers_driver_id drivers.driver_id
    drivers_driver_reference drivers.driver_reference
    drivers_number drivers.number
    drivers_code drivers.code
    drivers_forename drivers.forename
    drivers_surname drivers.surname
    drivers_dob drivers.dob
    drivers_nationality drivers.nationality
    drivers_url drivers.url
    status_id status.status_id
    status status.status
    seasons_year seasons.year
    seasons_url seasons.url
    constructors_constructor_id constructors.constructor_id
    constructors_constructor_reference constructors.constructor_reference
    constructors_name constructors.name
    constructors_nationality constructors.nationality
    constructors_url constructors.url
    constructor_standings_id constructor_standings_id
    constructor_standings_points points
    constructor_standings_position position
    constructor_standings_position_text position_text
    constructor_standings_wins wins
    results_result_id result_id
    results_number number
    results_grid grid
    results_position position
    results_position_text position_text
    results_position_order position_order
    results_points points
    results_laps laps
    results_time time
    results_milliseconds milliseconds
    results_fastest_lap fastest_lap
    results_rank rank
    results_fastest_lap_time fastest_lap_time
    results_fastest_lap_speed fastest_lap_speed
    results_status_id status_id
    driver_standings_id driver_standings_id
    driver_standings_points points
    driver_standings_position position
    driver_standings_position_text position_text
    driver_standings_wins wins
    constructor_results_id constructor_results_id
    constructor_results_points points
    constructor_results_status status
    qualifying_qualify_id qualify_id
    qualifying_number number
    qualifying_position position
    qualifying_q1 q1
    qualifying_q2 q2
    qualifying_q3 q3
    pit_stops_stop stop
    pit_stops_lap lap
    pit_stops_time time
    pit_stops_duration duration
    pit_stops_milliseconds milliseconds
    lap_times_lap lap
    lap_times_position position
    lap_times_time time
    lap_times_milliseconds milliseconds
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
