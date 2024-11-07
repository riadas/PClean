using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("chip model_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("chip model_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "model name"], Any[0, "launch year"], Any[0, "ram mib"], Any[0, "rom mib"], Any[0, "slots"], Any[0, "wifi"], Any[0, "bluetooth"], Any[1, "graphics mode"], Any[1, "char cells"], Any[1, "pixels"], Any[1, "hardware colours"], Any[1, "used kb"], Any[1, "map"], Any[1, "type"], Any[2, "company name"], Any[2, "hardware model name"], Any[2, "accreditation type"], Any[2, "accreditation level"], Any[2, "date"], Any[2, "chip model"], Any[2, "screen mode"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "model name"], Any[0, "launch year"], Any[0, "ram mib"], Any[0, "rom mib"], Any[0, "slots"], Any[0, "wifi"], Any[0, "bluetooth"], Any[1, "graphics mode"], Any[1, "char cells"], Any[1, "pixels"], Any[1, "hardware colours"], Any[1, "used kb"], Any[1, "map"], Any[1, "type"], Any[2, "company name"], Any[2, "hardware model name"], Any[2, "accreditation type"], Any[2, "accreditation level"], Any[2, "date"], Any[2, "chip model"], Any[2, "screen mode"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Phone1Model begin
    @class Chip_Model begin
        model_name ~ ChooseUniformly(possibilities[:model_name])
        launch_year ~ ChooseUniformly(possibilities[:launch_year])
        ram_mib ~ ChooseUniformly(possibilities[:ram_mib])
        rom_mib ~ ChooseUniformly(possibilities[:rom_mib])
        slots ~ ChooseUniformly(possibilities[:slots])
        wifi ~ ChooseUniformly(possibilities[:wifi])
        bluetooth ~ ChooseUniformly(possibilities[:bluetooth])
    end

    @class Screen_Mode begin
        graphics_mode ~ ChooseUniformly(possibilities[:graphics_mode])
        char_cells ~ ChooseUniformly(possibilities[:char_cells])
        pixels ~ ChooseUniformly(possibilities[:pixels])
        hardware_colours ~ ChooseUniformly(possibilities[:hardware_colours])
        used_kb ~ ChooseUniformly(possibilities[:used_kb])
        map ~ ChooseUniformly(possibilities[:map])
        type ~ ChooseUniformly(possibilities[:type])
    end

    @class Phone begin
        company_name ~ ChooseUniformly(possibilities[:company_name])
        hardware_model_name ~ ChooseUniformly(possibilities[:hardware_model_name])
        accreditation_type ~ ChooseUniformly(possibilities[:accreditation_type])
        accreditation_level ~ ChooseUniformly(possibilities[:accreditation_level])
        date ~ ChooseUniformly(possibilities[:date])
        chip_model ~ ChooseUniformly(possibilities[:chip_model])
        screen_mode ~ ChooseUniformly(possibilities[:screen_mode])
    end

    @class Obs begin
        chip_Model ~ Chip_Model
        screen_Mode ~ Screen_Mode
        phone ~ Phone
    end
end

query = @query Phone1Model.Obs [
    chip_model_model_name chip_Model.model_name
    chip_model_launch_year chip_Model.launch_year
    chip_model_ram_mib chip_Model.ram_mib
    chip_model_rom_mib chip_Model.rom_mib
    chip_model_slots chip_Model.slots
    chip_model_wifi chip_Model.wifi
    chip_model_bluetooth chip_Model.bluetooth
    screen_mode_graphics_mode screen_Mode.graphics_mode
    screen_mode_char_cells screen_Mode.char_cells
    screen_mode_pixels screen_Mode.pixels
    screen_mode_hardware_colours screen_Mode.hardware_colours
    screen_mode_used_kb screen_Mode.used_kb
    screen_mode_map screen_Mode.map
    screen_mode_type screen_Mode.type
    phone_company_name phone.company_name
    phone_hardware_model_name phone.hardware_model_name
    phone_accreditation_type phone.accreditation_type
    phone_accreditation_level phone.accreditation_level
    phone_date phone.date
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
