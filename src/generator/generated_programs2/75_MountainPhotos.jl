using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("mountain_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("mountain_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "id"], Any[1, "brand"], Any[1, "name"], Any[1, "focal length mm"], Any[1, "max aperture"], Any[2, "id"], Any[2, "camera lens id"], Any[2, "mountain id"], Any[2, "color"], Any[2, "name"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "id"], Any[1, "brand"], Any[1, "name"], Any[1, "focal length mm"], Any[1, "max aperture"], Any[2, "id"], Any[2, "camera lens id"], Any[2, "mountain id"], Any[2, "color"], Any[2, "name"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "id"], Any[1, "brand"], Any[1, "name"], Any[1, "focal length mm"], Any[1, "max aperture"], Any[2, "id"], Any[2, "camera lens id"], Any[2, "mountain id"], Any[2, "color"], Any[2, "name"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "id"], Any[1, "brand"], Any[1, "name"], Any[1, "focal length mm"], Any[1, "max aperture"], Any[2, "id"], Any[2, "camera lens id"], Any[2, "mountain id"], Any[2, "color"], Any[2, "name"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "id"], Any[1, "brand"], Any[1, "name"], Any[1, "focal length mm"], Any[1, "max aperture"], Any[2, "id"], Any[2, "camera lens id"], Any[2, "mountain id"], Any[2, "color"], Any[2, "name"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[14, 1], Any[13, 7]])
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







PClean.@model MountainPhotosModel begin
    @class Mountain begin
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        prominence ~ ChooseUniformly(possibilities[:prominence])
        range ~ ChooseUniformly(possibilities[:range])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Camera_lens begin
        brand ~ ChooseUniformly(possibilities[:brand])
        name ~ ChooseUniformly(possibilities[:name])
        focal_length_mm ~ ChooseUniformly(possibilities[:focal_length_mm])
        max_aperture ~ ChooseUniformly(possibilities[:max_aperture])
    end

    @class Photos begin
        camera_lens ~ Camera_lens
        mountain ~ Mountain
        color ~ ChooseUniformly(possibilities[:color])
        name ~ ChooseUniformly(possibilities[:name])
    end

    @class Obs begin
        photos ~ Photos
    end
end

query = @query MountainPhotosModel.Obs [
    mountain_name photos.mountain.name
    mountain_height photos.mountain.height
    mountain_prominence photos.mountain.prominence
    mountain_range photos.mountain.range
    mountain_country photos.mountain.country
    camera_lens_brand photos.camera_lens.brand
    camera_lens_name photos.camera_lens.name
    camera_lens_focal_length_mm photos.camera_lens.focal_length_mm
    camera_lens_max_aperture photos.camera_lens.max_aperture
    photos_color photos.color
    photos_name photos.name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
