using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("mountain_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("mountain_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
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
foreign_keys = ["mountain id", "camera lens id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "name"], Any[0, "height"], Any[0, "prominence"], Any[0, "range"], Any[0, "country"], Any[1, "id"], Any[1, "brand"], Any[1, "name"], Any[1, "focal length mm"], Any[1, "max aperture"], Any[2, "id"], Any[2, "color"], Any[2, "name"]]
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





PClean.@model MountainPhotosModel begin
    @class Mountain begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        height ~ ChooseUniformly(possibilities[:height])
        prominence ~ ChooseUniformly(possibilities[:prominence])
        range ~ ChooseUniformly(possibilities[:range])
        country ~ ChooseUniformly(possibilities[:country])
    end

    @class Camera_Lens begin
        id ~ Unmodeled()
        brand ~ ChooseUniformly(possibilities[:brand])
        name ~ ChooseUniformly(possibilities[:name])
        focal_length_mm ~ ChooseUniformly(possibilities[:focal_length_mm])
        max_aperture ~ ChooseUniformly(possibilities[:max_aperture])
    end

    @class Obs begin
        mountain ~ Mountain
        camera_Lens ~ Camera_Lens
        id ~ Unmodeled()
        color ~ ChooseUniformly(possibilities[:color])
        name ~ ChooseUniformly(possibilities[:name])
    end
end

query = @query MountainPhotosModel.Obs [
    mountain_id mountain.id
    mountain_name mountain.name
    mountain_height mountain.height
    mountain_prominence mountain.prominence
    mountain_range mountain.range
    mountain_country mountain.country
    camera_lens_id camera_Lens.id
    camera_lens_brand camera_Lens.brand
    camera_lens_name camera_Lens.name
    camera_lens_focal_length_mm camera_Lens.focal_length_mm
    camera_lens_max_aperture camera_Lens.max_aperture
    photos_id id
    photos_color color
    photos_name name
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
