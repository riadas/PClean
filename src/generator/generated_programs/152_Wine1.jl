using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("grapes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("grapes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "grape"], Any[0, "color"], Any[1, "no"], Any[1, "appelation"], Any[1, "county"], Any[1, "state"], Any[1, "area"], Any[1, "isava"], Any[2, "no"], Any[2, "grape"], Any[2, "winery"], Any[2, "appelation"], Any[2, "state"], Any[2, "name"], Any[2, "year"], Any[2, "price"], Any[2, "score"], Any[2, "cases"], Any[2, "drink"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "grape"], Any[0, "color"], Any[1, "no"], Any[1, "appelation"], Any[1, "county"], Any[1, "state"], Any[1, "area"], Any[1, "isava"], Any[2, "no"], Any[2, "grape"], Any[2, "winery"], Any[2, "appelation"], Any[2, "state"], Any[2, "name"], Any[2, "year"], Any[2, "price"], Any[2, "score"], Any[2, "cases"], Any[2, "drink"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Wine1Model begin
    @class Grapes begin
        id ~ Unmodeled()
        grape ~ ChooseUniformly(possibilities[:grape])
        color ~ ChooseUniformly(possibilities[:color])
    end

    @class Appellations begin
        no ~ ChooseUniformly(possibilities[:no])
        appelation ~ ChooseUniformly(possibilities[:appelation])
        county ~ ChooseUniformly(possibilities[:county])
        state ~ ChooseUniformly(possibilities[:state])
        area ~ ChooseUniformly(possibilities[:area])
        isava ~ ChooseUniformly(possibilities[:isava])
    end

    @class Wine begin
        no ~ ChooseUniformly(possibilities[:no])
        grape ~ ChooseUniformly(possibilities[:grape])
        winery ~ ChooseUniformly(possibilities[:winery])
        appelation ~ ChooseUniformly(possibilities[:appelation])
        state ~ ChooseUniformly(possibilities[:state])
        name ~ ChooseUniformly(possibilities[:name])
        year ~ ChooseUniformly(possibilities[:year])
        price ~ ChooseUniformly(possibilities[:price])
        score ~ ChooseUniformly(possibilities[:score])
        cases ~ ChooseUniformly(possibilities[:cases])
        drink ~ ChooseUniformly(possibilities[:drink])
    end

    @class Obs begin
        grapes ~ Grapes
        appellations ~ Appellations
        wine ~ Wine
    end
end

query = @query Wine1Model.Obs [
    grapes_id grapes.id
    grapes_grape grapes.grape
    grapes_color grapes.color
    appellations_no appellations.no
    appellations_appelation appellations.appelation
    appellations_county appellations.county
    appellations_state appellations.state
    appellations_area appellations.area
    appellations_isava appellations.isava
    wine_no wine.no
    winery wine.winery
    wine_state wine.state
    wine_name wine.name
    wine_year wine.year
    wine_price wine.price
    wine_score wine.score
    wine_cases wine.cases
    wine_drink wine.drink
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
