using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("datasets/rents_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("datasets/rents_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[0, "id"], Any[0, "room_type"], Any[0, "monthly_rent"], Any[0, "county"], Any[0, "state"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[0, "id"], Any[0, "room_type"], Any[0, "monthly_rent"], Any[0, "county"], Any[0, "state"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))



units = [Transformation(x -> x/1.0, x -> x*1.0, x -> 1/1.0), Transformation(x -> x/1000.0, x -> x*1000.0, x -> 1/1000.0)]

PClean.@model RentalDataModel begin
    @class Rental_listings begin
        room_type ~ ChooseUniformly(possibilities[:room_type])
        county ~ StringPrior(5, 35, possibilities[:county])
        state ~ ChooseUniformly(possibilities[:state])
    end

    @class Obs begin
        rental_listings ~ Rental_listings
        @learned avg_monthly_rent::Dict{String, MeanParameter{2068.443249159866, 1339.4564887947838}}
        unit ~ ChooseUniformly(units)
        monthly_rent_base = avg_monthly_rent["$(rental_listings.room_type)_$(rental_listings.county)_$(rental_listings.state)"]
        monthly_rent_corrected = round(unit.backward(monthly_rent))
        county ~ AddTypos(rental_listings.county, 2)
    end
end

query = @query RentalDataModel.Obs [
    "Room Type" rental_listings.room_type
    "Monthly Rent" monthly_rent_corrected monthly_rent
    County rental_listings.county county
    State rental_listings.state
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(1, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))