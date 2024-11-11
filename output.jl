using PClean
include("load_data.jl")

units = [Transformation(x -> x/1.0, x -> x*1.0, x -> 1/1.0), Transformation(x -> x/1000.0, x -> x*1000.0, x -> 1/1000.0)]

PClean.@model RentalDataModel begin
    @class Rental_listings begin
        id ~ Unmodeled()
        room_type ~ StringPrior(5, 35, possibilities[:room_type])
        monthly_rent ~ ChooseUniformly(possibilities[:monthly_rent])
        county ~ StringPrior(5, 35, possibilities[:county])
        state ~ StringPrior(5, 35, possibilities[:state])
    end

    @class Obs begin
        id ~ Rental_listings.id
        room_type ~ Rental_listings.room_type
        @learned avg_monthly_rent::Dict{String, MeanParameter{Statistics.mean(possibilities[:monthly_rent]), Statistics.std(possibilities[:monthly_rent])}}
        unit ~ ChooseUniformly(units)
        monthly_rent_base = avg_monthly_rent["$(Rental_listings.room_type)_$(Rental_listings.county)_$(Rental_listings.state)"]
        monthly_rent ~ TransformedGaussian(monthly_rent_base, Statistics.std(possibilities[:monthly_rent])/10, unit)
        monthly_rent_corrected = round(unit.backward(monthly_rent))
        county ~ AddTypos(Rental_listings.county, 2)
        state ~ Rental_listings.state
    end
end

query = @query RentalDataModel.Obs [
    rental_listings_id Rental_listings.id
    rental_listings_room_type Rental_listings.room_type
    rental_listings_monthly_rent monthly_rent_corrected monthly_rent
    rental_listings_county Rental_listings.county county
    rental_listings_state Rental_listings.state
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(1, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_data, tr.tables[:Obs], query))
