using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("datasets/rents_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("datasets/rents_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[0, "location_id"], Any[0, "county"], Any[0, "state"], Any[1, "column_id"], Any[1, "room_type"], Any[1, "monthly_rent"], Any[1, "location_id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[0, "location_id"], Any[0, "county"], Any[0, "state"], Any[1, "column_id"], Any[1, "room_type"], Any[1, "monthly_rent"], Any[1, "location_id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[0, "location_id"], Any[0, "county"], Any[0, "state"], Any[1, "column_id"], Any[1, "room_type"], Any[1, "monthly_rent"], Any[1, "location_id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[0, "location_id"], Any[0, "county"], Any[0, "state"], Any[1, "column_id"], Any[1, "room_type"], Any[1, "monthly_rent"], Any[1, "location_id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[0, "location_id"], Any[0, "county"], Any[0, "state"], Any[1, "column_id"], Any[1, "room_type"], Any[1, "monthly_rent"], Any[1, "location_id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[6, 0]])
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



units = [Transformation(x -> x/1* 1.0, x -> x*1*1.0, x -> 1/1*1.0), Transformation(x -> x/1000* 1.0, x -> x*1000*1.0, x -> 1/1000*1.0)]



PClean.@model RentalPropertiesModel begin
    @class Location begin
        county ~ StringPrior(5, 35, possibilities[:county])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Rental begin
        room_type ~ ChooseUniformly(possibilities[:room_type])
        location ~ Location
    end

    @class Obs begin
        rental ~ Rental
        county ~ AddTypos(rental.location.county, 2)
        @learned avg_monthly_rent::Dict{String, MeanParameter{2068.443249159866, 1339.4564887947838}}
        unit_monthly_rent ~ ChooseUniformly(units)
        monthly_rent_base = avg_monthly_rent["$(rental.room_type)"]
        monthly_rent ~ TransformedGaussian(monthly_rent_base, 1339.4564887947838/10, unit_monthly_rent)
        monthly_rent_corrected = round(unit_monthly_rent.backward(monthly_rent))
    end
end

query = @query RentalPropertiesModel.Obs [
    County rental.location.county county
    State rental.location.state
    "Room Type" rental.room_type
    "Monthly Rent" monthly_rent_corrected monthly_rent
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(1, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
