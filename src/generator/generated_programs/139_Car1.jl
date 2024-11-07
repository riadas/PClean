using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("continents_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("continents_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "cont id"], Any[0, "continent"], Any[1, "country id"], Any[1, "country name"], Any[1, "continent"], Any[2, "id"], Any[2, "maker"], Any[2, "full name"], Any[2, "country"], Any[3, "model id"], Any[3, "maker"], Any[3, "model"], Any[4, "make id"], Any[4, "model"], Any[4, "make"], Any[5, "id"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "cont id"], Any[0, "continent"], Any[1, "country id"], Any[1, "country name"], Any[1, "continent"], Any[2, "id"], Any[2, "maker"], Any[2, "full name"], Any[2, "country"], Any[3, "model id"], Any[3, "maker"], Any[3, "model"], Any[4, "make id"], Any[4, "model"], Any[4, "make"], Any[5, "id"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Car1Model begin
    @class Continents begin
        cont_id ~ Unmodeled()
        continent ~ ChooseUniformly(possibilities[:continent])
    end

    @class Countries begin
        country_id ~ Unmodeled()
        country_name ~ ChooseUniformly(possibilities[:country_name])
        continent ~ ChooseUniformly(possibilities[:continent])
    end

    @class Car_Makers begin
        id ~ Unmodeled()
        maker ~ ChooseUniformly(possibilities[:maker])
        full_name ~ ChooseUniformly(possibilities[:full_name])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Model_List begin
        model_id ~ Unmodeled()
        maker ~ ChooseUniformly(possibilities[:maker])
        model ~ ChooseUniformly(possibilities[:model])
    end

    @class Car_Names begin
        make_id ~ Unmodeled()
        model ~ ChooseUniformly(possibilities[:model])
        make ~ ChooseUniformly(possibilities[:make])
    end

    @class Cars_Data begin
        id ~ Unmodeled()
        mpg ~ ChooseUniformly(possibilities[:mpg])
        cylinders ~ ChooseUniformly(possibilities[:cylinders])
        edispl ~ ChooseUniformly(possibilities[:edispl])
        horsepower ~ ChooseUniformly(possibilities[:horsepower])
        weight ~ ChooseUniformly(possibilities[:weight])
        accelerate ~ ChooseUniformly(possibilities[:accelerate])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        continents ~ Continents
        countries ~ Countries
        car_Makers ~ Car_Makers
        model_List ~ Model_List
        car_Names ~ Car_Names
        cars_Data ~ Cars_Data
    end
end

query = @query Car1Model.Obs [
    continents_cont_id continents.cont_id
    continents_continent continents.continent
    countries_country_id countries.country_id
    countries_country_name countries.country_name
    car_makers_id car_Makers.id
    car_makers_maker car_Makers.maker
    car_makers_full_name car_Makers.full_name
    model_list_model_id model_List.model_id
    model_list_model model_List.model
    car_names_make_id car_Names.make_id
    car_names_make car_Names.make
    cars_data_mpg cars_Data.mpg
    cars_data_cylinders cars_Data.cylinders
    cars_data_edispl cars_Data.edispl
    cars_data_horsepower cars_Data.horsepower
    cars_data_weight cars_Data.weight
    cars_data_accelerate cars_Data.accelerate
    cars_data_year cars_Data.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
