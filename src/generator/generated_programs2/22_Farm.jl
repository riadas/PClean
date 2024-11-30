using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("city_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("city_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "city id"], Any[0, "official name"], Any[0, "status"], Any[0, "area km 2"], Any[0, "population"], Any[0, "census ranking"], Any[1, "farm id"], Any[1, "year"], Any[1, "total horses"], Any[1, "working horses"], Any[1, "total cattle"], Any[1, "oxen"], Any[1, "bulls"], Any[1, "cows"], Any[1, "pigs"], Any[1, "sheep and goats"], Any[2, "competition id"], Any[2, "year"], Any[2, "theme"], Any[2, "host city id"], Any[2, "hosts"], Any[3, "competition id"], Any[3, "farm id"], Any[3, "rank"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "city id"], Any[0, "official name"], Any[0, "status"], Any[0, "area km 2"], Any[0, "population"], Any[0, "census ranking"], Any[1, "farm id"], Any[1, "year"], Any[1, "total horses"], Any[1, "working horses"], Any[1, "total cattle"], Any[1, "oxen"], Any[1, "bulls"], Any[1, "cows"], Any[1, "pigs"], Any[1, "sheep and goats"], Any[2, "competition id"], Any[2, "year"], Any[2, "theme"], Any[2, "host city id"], Any[2, "hosts"], Any[3, "competition id"], Any[3, "farm id"], Any[3, "rank"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["host city id", "farm id", "competition id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "city id"], Any[0, "official name"], Any[0, "status"], Any[0, "area km 2"], Any[0, "population"], Any[0, "census ranking"], Any[1, "year"], Any[1, "total horses"], Any[1, "working horses"], Any[1, "total cattle"], Any[1, "oxen"], Any[1, "bulls"], Any[1, "cows"], Any[1, "pigs"], Any[1, "sheep and goats"], Any[2, "year"], Any[2, "theme"], Any[2, "hosts"], Any[3, "rank"]]
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
        city ~ City
        hosts ~ ChooseUniformly(possibilities[:hosts])
    end

    @class Competition_Record begin
        farm_Competition ~ Farm_Competition
        farm ~ Farm
        rank ~ ChooseUniformly(possibilities[:rank])
    end

    @class Obs begin
        competition_Record ~ Competition_Record
    end
end

query = @query FarmModel.Obs [
    city_id competition_Record.farm_Competition.city.city_id
    city_official_name competition_Record.farm_Competition.city.official_name
    city_status competition_Record.farm_Competition.city.status
    city_area_km_2 competition_Record.farm_Competition.city.area_km_2
    city_population competition_Record.farm_Competition.city.population
    city_census_ranking competition_Record.farm_Competition.city.census_ranking
    farm_id competition_Record.farm.farm_id
    farm_year competition_Record.farm.year
    farm_total_horses competition_Record.farm.total_horses
    farm_working_horses competition_Record.farm.working_horses
    farm_total_cattle competition_Record.farm.total_cattle
    farm_oxen competition_Record.farm.oxen
    farm_bulls competition_Record.farm.bulls
    farm_cows competition_Record.farm.cows
    farm_pigs competition_Record.farm.pigs
    farm_sheep_and_goats competition_Record.farm.sheep_and_goats
    farm_competition_competition_id competition_Record.farm_Competition.competition_id
    farm_competition_year competition_Record.farm_Competition.year
    farm_competition_theme competition_Record.farm_Competition.theme
    farm_competition_hosts competition_Record.farm_Competition.hosts
    competition_record_rank competition_Record.rank
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))