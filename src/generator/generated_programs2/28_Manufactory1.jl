using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("manufacturers_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("manufacturers_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "code"], Any[0, "name"], Any[0, "headquarter"], Any[0, "founder"], Any[0, "revenue"], Any[1, "code"], Any[1, "name"], Any[1, "price"], Any[1, "manufacturer"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "code"], Any[0, "name"], Any[0, "headquarter"], Any[0, "founder"], Any[0, "revenue"], Any[1, "code"], Any[1, "name"], Any[1, "price"], Any[1, "manufacturer"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["manufacturer"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "code"], Any[0, "name"], Any[0, "headquarter"], Any[0, "founder"], Any[0, "revenue"], Any[1, "code"], Any[1, "name"], Any[1, "price"]]
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





PClean.@model Manufactory1Model begin
    @class Manufacturers begin
        code ~ ChooseUniformly(possibilities[:code])
        name ~ ChooseUniformly(possibilities[:name])
        headquarter ~ ChooseUniformly(possibilities[:headquarter])
        founder ~ ChooseUniformly(possibilities[:founder])
        revenue ~ ChooseUniformly(possibilities[:revenue])
    end

    @class Products begin
        code ~ ChooseUniformly(possibilities[:code])
        name ~ ChooseUniformly(possibilities[:name])
        price ~ ChooseUniformly(possibilities[:price])
        manufacturers ~ Manufacturers
    end

    @class Obs begin
        products ~ Products
    end
end

query = @query Manufactory1Model.Obs [
    manufacturers_code products.manufacturers.code
    manufacturers_name products.manufacturers.name
    manufacturers_headquarter products.manufacturers.headquarter
    manufacturers_founder products.manufacturers.founder
    manufacturers_revenue products.manufacturers.revenue
    products_code products.code
    products_name products.name
    products_price products.price
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
