using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("rooms_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("rooms_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "room id"], Any[0, "room name"], Any[0, "beds"], Any[0, "bed type"], Any[0, "max occupancy"], Any[0, "base price"], Any[0, "decor"], Any[1, "code"], Any[1, "room"], Any[1, "check in"], Any[1, "check out"], Any[1, "rate"], Any[1, "last name"], Any[1, "first name"], Any[1, "adults"], Any[1, "kids"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "room id"], Any[0, "room name"], Any[0, "beds"], Any[0, "bed type"], Any[0, "max occupancy"], Any[0, "base price"], Any[0, "decor"], Any[1, "code"], Any[1, "room"], Any[1, "check in"], Any[1, "check out"], Any[1, "rate"], Any[1, "last name"], Any[1, "first name"], Any[1, "adults"], Any[1, "kids"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Inn1Model begin
    @class Rooms begin
        room_id ~ ChooseUniformly(possibilities[:room_id])
        room_name ~ ChooseUniformly(possibilities[:room_name])
        beds ~ ChooseUniformly(possibilities[:beds])
        bed_type ~ ChooseUniformly(possibilities[:bed_type])
        max_occupancy ~ ChooseUniformly(possibilities[:max_occupancy])
        base_price ~ ChooseUniformly(possibilities[:base_price])
        decor ~ ChooseUniformly(possibilities[:decor])
    end

    @class Reservations begin
        code ~ ChooseUniformly(possibilities[:code])
        room ~ ChooseUniformly(possibilities[:room])
        check_in ~ ChooseUniformly(possibilities[:check_in])
        check_out ~ ChooseUniformly(possibilities[:check_out])
        rate ~ ChooseUniformly(possibilities[:rate])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        adults ~ ChooseUniformly(possibilities[:adults])
        kids ~ ChooseUniformly(possibilities[:kids])
    end

    @class Obs begin
        rooms ~ Rooms
        reservations ~ Reservations
    end
end

query = @query Inn1Model.Obs [
    rooms_room_id rooms.room_id
    rooms_room_name rooms.room_name
    rooms_beds rooms.beds
    rooms_bed_type rooms.bed_type
    rooms_max_occupancy rooms.max_occupancy
    rooms_base_price rooms.base_price
    rooms_decor rooms.decor
    reservations_code reservations.code
    reservations_check_in reservations.check_in
    reservations_check_out reservations.check_out
    reservations_rate reservations.rate
    reservations_last_name reservations.last_name
    reservations_first_name reservations.first_name
    reservations_adults reservations.adults
    reservations_kids reservations.kids
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))