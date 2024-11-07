using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("city_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("city_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "city id"], Any[0, "official name"], Any[0, "status"], Any[0, "area km 2"], Any[0, "population"], Any[0, "census ranking"], Any[1, "farm id"], Any[1, "year"], Any[1, "total horses"], Any[1, "working horses"], Any[1, "total cattle"], Any[1, "oxen"], Any[1, "bulls"], Any[1, "cows"], Any[1, "pigs"], Any[1, "sheep and goats"], Any[2, "competition id"], Any[2, "year"], Any[2, "theme"], Any[2, "host city id"], Any[2, "hosts"], Any[3, "competition id"], Any[3, "farm id"], Any[3, "rank"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "city id"], Any[0, "official name"], Any[0, "status"], Any[0, "area km 2"], Any[0, "population"], Any[0, "census ranking"], Any[1, "farm id"], Any[1, "year"], Any[1, "total horses"], Any[1, "working horses"], Any[1, "total cattle"], Any[1, "oxen"], Any[1, "bulls"], Any[1, "cows"], Any[1, "pigs"], Any[1, "sheep and goats"], Any[2, "competition id"], Any[2, "year"], Any[2, "theme"], Any[2, "host city id"], Any[2, "hosts"], Any[3, "competition id"], Any[3, "farm id"], Any[3, "rank"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model FarmModel begin
    @class City begin
        city_id ~ Unmodeled()
        official_name ~ ChooseUniformly(possibilities[:official_name])
        status ~ ChooseUniformly(possibilities[:status])
        area_km_2 ~ ChooseUniformly(possibilities[:area_km_2])
        population ~ ChooseUniformly(possibilities[:population])
        census_ranking ~ ChooseUniformly(possibilities[:census_ranking])
    end

    @class Farm begin
        farm_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        total_horses ~ ChooseUniformly(possibilities[:total_horses])
        working_horses ~ ChooseUniformly(possibilities[:working_horses])
        total_cattle ~ ChooseUniformly(possibilities[:total_cattle])
        oxen ~ ChooseUniformly(possibilities[:oxen])
        bulls ~ ChooseUniformly(possibilities[:bulls])
        cows ~ ChooseUniformly(possibilities[:cows])
        pigs ~ ChooseUniformly(possibilities[:pigs])
        sheep_and_goats ~ ChooseUniformly(possibilities[:sheep_and_goats])
    end

    @class Farm_Competition begin
        competition_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        theme ~ ChooseUniformly(possibilities[:theme])
        host_city_id ~ ChooseUniformly(possibilities[:host_city_id])
        hosts ~ ChooseUniformly(possibilities[:hosts])
    end

    @class Competition_Record begin
        competition_id ~ Unmodeled()
        farm_id ~ ChooseUniformly(possibilities[:farm_id])
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Obs begin
        city ~ City
        farm ~ Farm
        farm_Competition ~ Farm_Competition
        competition_Record ~ Competition_Record
    end
end

query = @query FarmModel.Obs [
    city_id city.city_id
    city_official_name city.official_name
    city_status city.status
    city_area_km_2 city.area_km_2
    city_population city.population
    city_census_ranking city.census_ranking
    farm_id farm.farm_id
    farm_year farm.year
    farm_total_horses farm.total_horses
    farm_working_horses farm.working_horses
    farm_total_cattle farm.total_cattle
    farm_oxen farm.oxen
    farm_bulls farm.bulls
    farm_cows farm.cows
    farm_pigs farm.pigs
    farm_sheep_and_goats farm.sheep_and_goats
    farm_competition_competition_id farm_Competition.competition_id
    farm_competition_year farm_Competition.year
    farm_competition_theme farm_Competition.theme
    farm_competition_hosts farm_Competition.hosts
    competition_record_rank competition_Record.rank
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
