using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("grapes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("grapes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "grape"], Any[0, "color"], Any[1, "no"], Any[1, "appelation"], Any[1, "county"], Any[1, "state"], Any[1, "area"], Any[1, "isava"], Any[2, "no"], Any[2, "grape"], Any[2, "winery"], Any[2, "appelation"], Any[2, "state"], Any[2, "name"], Any[2, "year"], Any[2, "price"], Any[2, "score"], Any[2, "cases"], Any[2, "drink"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "grape"], Any[0, "color"], Any[1, "no"], Any[1, "appelation"], Any[1, "county"], Any[1, "state"], Any[1, "area"], Any[1, "isava"], Any[2, "no"], Any[2, "grape"], Any[2, "winery"], Any[2, "appelation"], Any[2, "state"], Any[2, "name"], Any[2, "year"], Any[2, "price"], Any[2, "score"], Any[2, "cases"], Any[2, "drink"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "grape"], Any[0, "color"], Any[1, "no"], Any[1, "appelation"], Any[1, "county"], Any[1, "state"], Any[1, "area"], Any[1, "isava"], Any[2, "no"], Any[2, "grape"], Any[2, "winery"], Any[2, "appelation"], Any[2, "state"], Any[2, "name"], Any[2, "year"], Any[2, "price"], Any[2, "score"], Any[2, "cases"], Any[2, "drink"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "grape"], Any[0, "color"], Any[1, "no"], Any[1, "appelation"], Any[1, "county"], Any[1, "state"], Any[1, "area"], Any[1, "isava"], Any[2, "no"], Any[2, "grape"], Any[2, "winery"], Any[2, "appelation"], Any[2, "state"], Any[2, "name"], Any[2, "year"], Any[2, "price"], Any[2, "score"], Any[2, "cases"], Any[2, "drink"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "grape"], Any[0, "color"], Any[1, "no"], Any[1, "appelation"], Any[1, "county"], Any[1, "state"], Any[1, "area"], Any[1, "isava"], Any[2, "no"], Any[2, "grape"], Any[2, "winery"], Any[2, "appelation"], Any[2, "state"], Any[2, "name"], Any[2, "year"], Any[2, "price"], Any[2, "score"], Any[2, "cases"], Any[2, "drink"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[13, 5], Any[11, 2]])
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







PClean.@model Wine1Model begin
    @class Grapes begin
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
        grapes ~ Grapes
        winery ~ ChooseUniformly(possibilities[:winery])
        appellations ~ Appellations
        state ~ ChooseUniformly(possibilities[:state])
        name ~ ChooseUniformly(possibilities[:name])
        year ~ ChooseUniformly(possibilities[:year])
        price ~ ChooseUniformly(possibilities[:price])
        score ~ ChooseUniformly(possibilities[:score])
        cases ~ ChooseUniformly(possibilities[:cases])
        drink ~ ChooseUniformly(possibilities[:drink])
    end

    @class Obs begin
        wine ~ Wine
    end
end

query = @query Wine1Model.Obs [
    grapes_grape wine.grapes.grape
    grapes_color wine.grapes.color
    appellations_no wine.appellations.no
    appellations_appelation wine.appellations.appelation
    appellations_county wine.appellations.county
    appellations_state wine.appellations.state
    appellations_area wine.appellations.area
    appellations_isava wine.appellations.isava
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
