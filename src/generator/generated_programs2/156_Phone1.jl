using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("chip_model_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("chip_model_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "model name"], Any[0, "launch year"], Any[0, "ram mib"], Any[0, "rom mib"], Any[0, "slots"], Any[0, "wifi"], Any[0, "bluetooth"], Any[1, "graphics mode"], Any[1, "char cells"], Any[1, "pixels"], Any[1, "hardware colours"], Any[1, "used kb"], Any[1, "map"], Any[1, "type"], Any[2, "company name"], Any[2, "hardware model name"], Any[2, "accreditation type"], Any[2, "accreditation level"], Any[2, "date"], Any[2, "chip model"], Any[2, "screen mode"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "model name"], Any[0, "launch year"], Any[0, "ram mib"], Any[0, "rom mib"], Any[0, "slots"], Any[0, "wifi"], Any[0, "bluetooth"], Any[1, "graphics mode"], Any[1, "char cells"], Any[1, "pixels"], Any[1, "hardware colours"], Any[1, "used kb"], Any[1, "map"], Any[1, "type"], Any[2, "company name"], Any[2, "hardware model name"], Any[2, "accreditation type"], Any[2, "accreditation level"], Any[2, "date"], Any[2, "chip model"], Any[2, "screen mode"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "model name"], Any[0, "launch year"], Any[0, "ram mib"], Any[0, "rom mib"], Any[0, "slots"], Any[0, "wifi"], Any[0, "bluetooth"], Any[1, "graphics mode"], Any[1, "char cells"], Any[1, "pixels"], Any[1, "hardware colours"], Any[1, "used kb"], Any[1, "map"], Any[1, "type"], Any[2, "company name"], Any[2, "hardware model name"], Any[2, "accreditation type"], Any[2, "accreditation level"], Any[2, "date"], Any[2, "chip model"], Any[2, "screen mode"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "model name"], Any[0, "launch year"], Any[0, "ram mib"], Any[0, "rom mib"], Any[0, "slots"], Any[0, "wifi"], Any[0, "bluetooth"], Any[1, "graphics mode"], Any[1, "char cells"], Any[1, "pixels"], Any[1, "hardware colours"], Any[1, "used kb"], Any[1, "map"], Any[1, "type"], Any[2, "company name"], Any[2, "hardware model name"], Any[2, "accreditation type"], Any[2, "accreditation level"], Any[2, "date"], Any[2, "chip model"], Any[2, "screen mode"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "model name"], Any[0, "launch year"], Any[0, "ram mib"], Any[0, "rom mib"], Any[0, "slots"], Any[0, "wifi"], Any[0, "bluetooth"], Any[1, "graphics mode"], Any[1, "char cells"], Any[1, "pixels"], Any[1, "hardware colours"], Any[1, "used kb"], Any[1, "map"], Any[1, "type"], Any[2, "company name"], Any[2, "hardware model name"], Any[2, "accreditation type"], Any[2, "accreditation level"], Any[2, "date"], Any[2, "chip model"], Any[2, "screen mode"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[20, 1], Any[21, 8]])
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







PClean.@model Phone1Model begin
    @class Chip_model begin
        model_name ~ ChooseUniformly(possibilities[:model_name])
        launch_year ~ ChooseUniformly(possibilities[:launch_year])
        ram_mib ~ ChooseUniformly(possibilities[:ram_mib])
        rom_mib ~ ChooseUniformly(possibilities[:rom_mib])
        slots ~ ChooseUniformly(possibilities[:slots])
        wifi ~ ChooseUniformly(possibilities[:wifi])
        bluetooth ~ ChooseUniformly(possibilities[:bluetooth])
    end

    @class Screen_mode begin
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
        chip_model ~ Chip_model
        screen_mode ~ Screen_mode
    end

    @class Obs begin
        phone ~ Phone
    end
end

query = @query Phone1Model.Obs [
    chip_model_model_name phone.chip_model.model_name
    chip_model_launch_year phone.chip_model.launch_year
    chip_model_ram_mib phone.chip_model.ram_mib
    chip_model_rom_mib phone.chip_model.rom_mib
    chip_model_slots phone.chip_model.slots
    chip_model_wifi phone.chip_model.wifi
    chip_model_bluetooth phone.chip_model.bluetooth
    screen_mode_graphics_mode phone.screen_mode.graphics_mode
    screen_mode_char_cells phone.screen_mode.char_cells
    screen_mode_pixels phone.screen_mode.pixels
    screen_mode_hardware_colours phone.screen_mode.hardware_colours
    screen_mode_used_kb phone.screen_mode.used_kb
    screen_mode_map phone.screen_mode.map
    screen_mode_type phone.screen_mode.type
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
