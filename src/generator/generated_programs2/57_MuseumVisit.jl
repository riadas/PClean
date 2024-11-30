using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("museum_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("museum_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "museum id"], Any[0, "name"], Any[0, "num of staff"], Any[0, "open year"], Any[1, "customer id"], Any[1, "name"], Any[1, "level of membership"], Any[1, "age"], Any[2, "museum id"], Any[2, "customer id"], Any[2, "num of ticket"], Any[2, "total spent"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "museum id"], Any[0, "name"], Any[0, "num of staff"], Any[0, "open year"], Any[1, "customer id"], Any[1, "name"], Any[1, "level of membership"], Any[1, "age"], Any[2, "museum id"], Any[2, "customer id"], Any[2, "num of ticket"], Any[2, "total spent"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["customer id", "museum id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "num of staff"], Any[0, "open year"], Any[1, "name"], Any[1, "level of membership"], Any[1, "age"], Any[2, "num of ticket"], Any[2, "total spent"]]
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





PClean.@model MuseumVisitModel begin
    @class Museum begin
        museum_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        num_of_staff ~ ChooseUniformly(possibilities[:num_of_staff])
        open_year ~ ChooseUniformly(possibilities[:open_year])
    end

    @class Customer begin
        customer_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        level_of_membership ~ ChooseUniformly(possibilities[:level_of_membership])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Visit begin
        museum ~ Museum
        customer ~ Customer
        num_of_ticket ~ ChooseUniformly(possibilities[:num_of_ticket])
        total_spent ~ ChooseUniformly(possibilities[:total_spent])
    end

    @class Obs begin
        visit ~ Visit
    end
end

query = @query MuseumVisitModel.Obs [
    museum_id visit.museum.museum_id
    museum_name visit.museum.name
    museum_num_of_staff visit.museum.num_of_staff
    museum_open_year visit.museum.open_year
    customer_id visit.customer.customer_id
    customer_name visit.customer.name
    customer_level_of_membership visit.customer.level_of_membership
    customer_age visit.customer.age
    visit_num_of_ticket visit.num_of_ticket
    visit_total_spent visit.total_spent
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
