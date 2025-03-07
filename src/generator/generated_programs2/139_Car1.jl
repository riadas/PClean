using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("continents_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("continents_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "cont id"], Any[0, "continent"], Any[1, "country id"], Any[1, "country name"], Any[1, "continent"], Any[2, "id"], Any[2, "maker"], Any[2, "full name"], Any[2, "country"], Any[3, "model id"], Any[3, "maker"], Any[3, "model"], Any[4, "make id"], Any[4, "model"], Any[4, "make"], Any[5, "id"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "cont id"], Any[0, "continent"], Any[1, "country id"], Any[1, "country name"], Any[1, "continent"], Any[2, "id"], Any[2, "maker"], Any[2, "full name"], Any[2, "country"], Any[3, "model id"], Any[3, "maker"], Any[3, "model"], Any[4, "make id"], Any[4, "model"], Any[4, "make"], Any[5, "id"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "cont id"], Any[0, "continent"], Any[1, "country id"], Any[1, "country name"], Any[1, "continent"], Any[2, "id"], Any[2, "maker"], Any[2, "full name"], Any[2, "country"], Any[3, "model id"], Any[3, "maker"], Any[3, "model"], Any[4, "make id"], Any[4, "model"], Any[4, "make"], Any[5, "id"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "cont id"], Any[0, "continent"], Any[1, "country id"], Any[1, "country name"], Any[1, "continent"], Any[2, "id"], Any[2, "maker"], Any[2, "full name"], Any[2, "country"], Any[3, "model id"], Any[3, "maker"], Any[3, "model"], Any[4, "make id"], Any[4, "model"], Any[4, "make"], Any[5, "id"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "cont id"], Any[0, "continent"], Any[1, "country id"], Any[1, "country name"], Any[1, "continent"], Any[2, "id"], Any[2, "maker"], Any[2, "full name"], Any[2, "country"], Any[3, "model id"], Any[3, "maker"], Any[3, "model"], Any[4, "make id"], Any[4, "model"], Any[4, "make"], Any[5, "id"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[5, 1], Any[9, 3], Any[11, 6], Any[14, 12], Any[16, 13]])
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







PClean.@model Car1Model begin
    @class Continents begin
        continent ~ ChooseUniformly(possibilities[:continent])
    end

    @class Countries begin
        country_name ~ ChooseUniformly(possibilities[:country_name])
        continents ~ Continents
    end

    @class Car_makers begin
        maker ~ ChooseUniformly(possibilities[:maker])
        full_name ~ ChooseUniformly(possibilities[:full_name])
        countries ~ Countries
    end

    @class Model_list begin
        car_makers ~ Car_makers
        model ~ ChooseUniformly(possibilities[:model])
    end

    @class Car_names begin
        model_list ~ Model_list
        make ~ ChooseUniformly(possibilities[:make])
    end

    @class Cars_data begin
        mpg ~ ChooseUniformly(possibilities[:mpg])
        cylinders ~ ChooseUniformly(possibilities[:cylinders])
        edispl ~ ChooseUniformly(possibilities[:edispl])
        horsepower ~ ChooseUniformly(possibilities[:horsepower])
        weight ~ ChooseUniformly(possibilities[:weight])
        accelerate ~ ChooseUniformly(possibilities[:accelerate])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        cars_data ~ Cars_data
    end
end

query = @query Car1Model.Obs [
    continents_cont_id cars_data.car_names.model_list.car_makers.countries.continents.cont_id
    continents_continent cars_data.car_names.model_list.car_makers.countries.continents.continent
    countries_country_id cars_data.car_names.model_list.car_makers.countries.country_id
    countries_country_name cars_data.car_names.model_list.car_makers.countries.country_name
    car_makers_maker cars_data.car_names.model_list.car_makers.maker
    car_makers_full_name cars_data.car_names.model_list.car_makers.full_name
    model_list_model_id cars_data.car_names.model_list.model_id
    model_list_model cars_data.car_names.model_list.model
    car_names_make_id cars_data.car_names.make_id
    car_names_make cars_data.car_names.make
    cars_data_mpg cars_data.mpg
    cars_data_cylinders cars_data.cylinders
    cars_data_edispl cars_data.edispl
    cars_data_horsepower cars_data.horsepower
    cars_data_weight cars_data.weight
    cars_data_accelerate cars_data.accelerate
    cars_data_year cars_data.year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
