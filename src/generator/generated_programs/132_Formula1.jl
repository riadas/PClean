using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("circuits_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("circuits_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "circuit id"], Any[0, "circuit reference"], Any[0, "name"], Any[0, "location"], Any[0, "country"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "altitude"], Any[0, "url"], Any[1, "race id"], Any[1, "year"], Any[1, "round"], Any[1, "circuit id"], Any[1, "name"], Any[1, "date"], Any[1, "time"], Any[1, "url"], Any[2, "driver id"], Any[2, "driver reference"], Any[2, "number"], Any[2, "code"], Any[2, "forename"], Any[2, "surname"], Any[2, "dob"], Any[2, "nationality"], Any[2, "url"], Any[3, "status id"], Any[3, "status"], Any[4, "year"], Any[4, "url"], Any[5, "constructor id"], Any[5, "constructor reference"], Any[5, "name"], Any[5, "nationality"], Any[5, "url"], Any[6, "constructor standings id"], Any[6, "race id"], Any[6, "constructor id"], Any[6, "points"], Any[6, "position"], Any[6, "position text"], Any[6, "wins"], Any[7, "result id"], Any[7, "race id"], Any[7, "driver id"], Any[7, "constructor id"], Any[7, "number"], Any[7, "grid"], Any[7, "position"], Any[7, "position text"], Any[7, "position order"], Any[7, "points"], Any[7, "laps"], Any[7, "time"], Any[7, "milliseconds"], Any[7, "fastest lap"], Any[7, "rank"], Any[7, "fastest lap time"], Any[7, "fastest lap speed"], Any[7, "status id"], Any[8, "driver standings id"], Any[8, "race id"], Any[8, "driver id"], Any[8, "points"], Any[8, "position"], Any[8, "position text"], Any[8, "wins"], Any[9, "constructor results id"], Any[9, "race id"], Any[9, "constructor id"], Any[9, "points"], Any[9, "status"], Any[10, "qualify id"], Any[10, "race id"], Any[10, "driver id"], Any[10, "constructor id"], Any[10, "number"], Any[10, "position"], Any[10, "q1"], Any[10, "q2"], Any[10, "q3"], Any[11, "race id"], Any[11, "driver id"], Any[11, "stop"], Any[11, "lap"], Any[11, "time"], Any[11, "duration"], Any[11, "milliseconds"], Any[12, "race id"], Any[12, "driver id"], Any[12, "lap"], Any[12, "position"], Any[12, "time"], Any[12, "milliseconds"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "circuit id"], Any[0, "circuit reference"], Any[0, "name"], Any[0, "location"], Any[0, "country"], Any[0, "latitude"], Any[0, "longitude"], Any[0, "altitude"], Any[0, "url"], Any[1, "race id"], Any[1, "year"], Any[1, "round"], Any[1, "circuit id"], Any[1, "name"], Any[1, "date"], Any[1, "time"], Any[1, "url"], Any[2, "driver id"], Any[2, "driver reference"], Any[2, "number"], Any[2, "code"], Any[2, "forename"], Any[2, "surname"], Any[2, "dob"], Any[2, "nationality"], Any[2, "url"], Any[3, "status id"], Any[3, "status"], Any[4, "year"], Any[4, "url"], Any[5, "constructor id"], Any[5, "constructor reference"], Any[5, "name"], Any[5, "nationality"], Any[5, "url"], Any[6, "constructor standings id"], Any[6, "race id"], Any[6, "constructor id"], Any[6, "points"], Any[6, "position"], Any[6, "position text"], Any[6, "wins"], Any[7, "result id"], Any[7, "race id"], Any[7, "driver id"], Any[7, "constructor id"], Any[7, "number"], Any[7, "grid"], Any[7, "position"], Any[7, "position text"], Any[7, "position order"], Any[7, "points"], Any[7, "laps"], Any[7, "time"], Any[7, "milliseconds"], Any[7, "fastest lap"], Any[7, "rank"], Any[7, "fastest lap time"], Any[7, "fastest lap speed"], Any[7, "status id"], Any[8, "driver standings id"], Any[8, "race id"], Any[8, "driver id"], Any[8, "points"], Any[8, "position"], Any[8, "position text"], Any[8, "wins"], Any[9, "constructor results id"], Any[9, "race id"], Any[9, "constructor id"], Any[9, "points"], Any[9, "status"], Any[10, "qualify id"], Any[10, "race id"], Any[10, "driver id"], Any[10, "constructor id"], Any[10, "number"], Any[10, "position"], Any[10, "q1"], Any[10, "q2"], Any[10, "q3"], Any[11, "race id"], Any[11, "driver id"], Any[11, "stop"], Any[11, "lap"], Any[11, "time"], Any[11, "duration"], Any[11, "milliseconds"], Any[12, "race id"], Any[12, "driver id"], Any[12, "lap"], Any[12, "position"], Any[12, "time"], Any[12, "milliseconds"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Races begin
        race_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        round ~ ChooseUniformly(possibilities[:round])
        circuit_id ~ ChooseUniformly(possibilities[:circuit_id])
        name ~ ChooseUniformly(possibilities[:name])
        date ~ ChooseUniformly(possibilities[:date])
        time ~ ChooseUniformly(possibilities[:time])
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

    @class Constructor_Standings begin
        constructor_standings_id ~ Unmodeled()
        race_id ~ ChooseUniformly(possibilities[:race_id])
        constructor_id ~ ChooseUniformly(possibilities[:constructor_id])
        points ~ ChooseUniformly(possibilities[:points])
        position ~ ChooseUniformly(possibilities[:position])
        position_text ~ ChooseUniformly(possibilities[:position_text])
        wins ~ ChooseUniformly(possibilities[:wins])
    end

    @class Results begin
        result_id ~ Unmodeled()
        race_id ~ ChooseUniformly(possibilities[:race_id])
        driver_id ~ ChooseUniformly(possibilities[:driver_id])
        constructor_id ~ ChooseUniformly(possibilities[:constructor_id])
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
    end

    @class Driver_Standings begin
        driver_standings_id ~ Unmodeled()
        race_id ~ ChooseUniformly(possibilities[:race_id])
        driver_id ~ ChooseUniformly(possibilities[:driver_id])
        points ~ ChooseUniformly(possibilities[:points])
        position ~ ChooseUniformly(possibilities[:position])
        position_text ~ ChooseUniformly(possibilities[:position_text])
        wins ~ ChooseUniformly(possibilities[:wins])
    end

    @class Constructor_Results begin
        constructor_results_id ~ Unmodeled()
        race_id ~ ChooseUniformly(possibilities[:race_id])
        constructor_id ~ ChooseUniformly(possibilities[:constructor_id])
        points ~ ChooseUniformly(possibilities[:points])
        status ~ ChooseUniformly(possibilities[:status])
    end

    @class Qualifying begin
        qualify_id ~ Unmodeled()
        race_id ~ ChooseUniformly(possibilities[:race_id])
        driver_id ~ ChooseUniformly(possibilities[:driver_id])
        constructor_id ~ ChooseUniformly(possibilities[:constructor_id])
        number ~ ChooseUniformly(possibilities[:number])
        position ~ ChooseUniformly(possibilities[:position])
        q1 ~ ChooseUniformly(possibilities[:q1])
        q2 ~ ChooseUniformly(possibilities[:q2])
        q3 ~ ChooseUniformly(possibilities[:q3])
    end

    @class Pit_Stops begin
        race_id ~ Unmodeled()
        driver_id ~ ChooseUniformly(possibilities[:driver_id])
        stop ~ ChooseUniformly(possibilities[:stop])
        lap ~ ChooseUniformly(possibilities[:lap])
        time ~ ChooseUniformly(possibilities[:time])
        duration ~ ChooseUniformly(possibilities[:duration])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
    end

    @class Lap_Times begin
        race_id ~ Unmodeled()
        driver_id ~ ChooseUniformly(possibilities[:driver_id])
        lap ~ ChooseUniformly(possibilities[:lap])
        position ~ ChooseUniformly(possibilities[:position])
        time ~ ChooseUniformly(possibilities[:time])
        milliseconds ~ ChooseUniformly(possibilities[:milliseconds])
    end

    @class Obs begin
        circuits ~ Circuits
        races ~ Races
        drivers ~ Drivers
        status ~ Status
        seasons ~ Seasons
        constructors ~ Constructors
        constructor_Standings ~ Constructor_Standings
        results ~ Results
        driver_Standings ~ Driver_Standings
        constructor_Results ~ Constructor_Results
        qualifying ~ Qualifying
        pit_Stops ~ Pit_Stops
        lap_Times ~ Lap_Times
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
    races_race_id races.race_id
    races_year races.year
    races_round races.round
    races_name races.name
    races_date races.date
    races_time races.time
    races_url races.url
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
    constructor_standings_id constructor_Standings.constructor_standings_id
    constructor_standings_points constructor_Standings.points
    constructor_standings_position constructor_Standings.position
    constructor_standings_position_text constructor_Standings.position_text
    constructor_standings_wins constructor_Standings.wins
    results_result_id results.result_id
    results_number results.number
    results_grid results.grid
    results_position results.position
    results_position_text results.position_text
    results_position_order results.position_order
    results_points results.points
    results_laps results.laps
    results_time results.time
    results_milliseconds results.milliseconds
    results_fastest_lap results.fastest_lap
    results_rank results.rank
    results_fastest_lap_time results.fastest_lap_time
    results_fastest_lap_speed results.fastest_lap_speed
    results_status_id results.status_id
    driver_standings_id driver_Standings.driver_standings_id
    driver_standings_points driver_Standings.points
    driver_standings_position driver_Standings.position
    driver_standings_position_text driver_Standings.position_text
    driver_standings_wins driver_Standings.wins
    constructor_results_id constructor_Results.constructor_results_id
    constructor_results_points constructor_Results.points
    constructor_results_status constructor_Results.status
    qualifying_qualify_id qualifying.qualify_id
    qualifying_number qualifying.number
    qualifying_position qualifying.position
    qualifying_q1 qualifying.q1
    qualifying_q2 qualifying.q2
    qualifying_q3 qualifying.q3
    pit_stops_stop pit_Stops.stop
    pit_stops_lap pit_Stops.lap
    pit_stops_time pit_Stops.time
    pit_stops_duration pit_Stops.duration
    pit_stops_milliseconds pit_Stops.milliseconds
    lap_times_lap lap_Times.lap
    lap_times_position lap_Times.position
    lap_times_time lap_Times.time
    lap_times_milliseconds lap_Times.milliseconds
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
