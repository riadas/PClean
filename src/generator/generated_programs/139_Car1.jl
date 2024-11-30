using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("continents_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("continents_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["continent", "country", "maker", "model", "id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "cont id"], Any[1, "country id"], Any[1, "country name"], Any[2, "full name"], Any[3, "model id"], Any[4, "make id"], Any[4, "make"], Any[5, "mpg"], Any[5, "cylinders"], Any[5, "edispl"], Any[5, "horsepower"], Any[5, "weight"], Any[5, "accelerate"], Any[5, "year"]]
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





PClean.@model Car1Model begin
    @class Continents begin
        cont_id ~ Unmodeled()
        continent ~ ChooseUniformly(possibilities[:continent])
    end

    @class Obs begin
        continents ~ Continents
        country_id ~ Unmodeled()
        country_name ~ ChooseUniformly(possibilities[:country_name])
        id ~ Unmodeled()
        maker ~ ChooseUniformly(possibilities[:maker])
        full_name ~ ChooseUniformly(possibilities[:full_name])
        model_id ~ Unmodeled()
        model ~ ChooseUniformly(possibilities[:model])
        make_id ~ Unmodeled()
        make ~ ChooseUniformly(possibilities[:make])
        mpg ~ ChooseUniformly(possibilities[:mpg])
        cylinders ~ ChooseUniformly(possibilities[:cylinders])
        edispl ~ ChooseUniformly(possibilities[:edispl])
        horsepower ~ ChooseUniformly(possibilities[:horsepower])
        weight ~ ChooseUniformly(possibilities[:weight])
        accelerate ~ ChooseUniformly(possibilities[:accelerate])
        year ~ ChooseUniformly(possibilities[:year])
    end
end

query = @query Car1Model.Obs [
    continents_cont_id continents.cont_id
    continents_continent continents.continent
    countries_country_id country_id
    countries_country_name country_name
    car_makers_id id
    car_makers_maker maker
    car_makers_full_name full_name
    model_list_model_id model_id
    model_list_model model
    car_names_make_id make_id
    car_names_make make
    cars_data_mpg mpg
    cars_data_cylinders cylinders
    cars_data_edispl edispl
    cars_data_horsepower horsepower
    cars_data_weight weight
    cars_data_accelerate accelerate
    cars_data_year year
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
