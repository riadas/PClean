using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("campuses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("campuses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "id"], Any[0, "campus"], Any[0, "location"], Any[0, "county"], Any[0, "year"], Any[1, "campus"], Any[1, "year"], Any[1, "campus fee"], Any[2, "year"], Any[2, "campus"], Any[2, "degrees"], Any[3, "campus"], Any[3, "discipline"], Any[3, "year"], Any[3, "undergraduate"], Any[3, "graduate"], Any[4, "campus"], Any[4, "year"], Any[4, "totalenrollment ay"], Any[4, "fte ay"], Any[5, "campus"], Any[5, "year"], Any[5, "faculty"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "id"], Any[0, "campus"], Any[0, "location"], Any[0, "county"], Any[0, "year"], Any[1, "campus"], Any[1, "year"], Any[1, "campus fee"], Any[2, "year"], Any[2, "campus"], Any[2, "degrees"], Any[3, "campus"], Any[3, "discipline"], Any[3, "year"], Any[3, "undergraduate"], Any[3, "graduate"], Any[4, "campus"], Any[4, "year"], Any[4, "totalenrollment ay"], Any[4, "fte ay"], Any[5, "campus"], Any[5, "year"], Any[5, "faculty"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["campus", "campus", "campus", "campus", "campus"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "id"], Any[0, "location"], Any[0, "county"], Any[0, "year"], Any[1, "year"], Any[1, "campus fee"], Any[2, "year"], Any[2, "degrees"], Any[3, "discipline"], Any[3, "year"], Any[3, "undergraduate"], Any[3, "graduate"], Any[4, "year"], Any[4, "totalenrollment ay"], Any[4, "fte ay"], Any[5, "year"], Any[5, "faculty"]]
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





PClean.@model Csu1Model begin
    @class Campuses begin
        id ~ Unmodeled()
        campus ~ ChooseUniformly(possibilities[:campus])
        location ~ ChooseUniformly(possibilities[:location])
        county ~ ChooseUniformly(possibilities[:county])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Obs begin
        campuses ~ Campuses
        year ~ ChooseUniformly(possibilities[:year])
        campus_fee ~ ChooseUniformly(possibilities[:campus_fee])
        year ~ ChooseUniformly(possibilities[:year])
        degrees ~ ChooseUniformly(possibilities[:degrees])
        discipline ~ ChooseUniformly(possibilities[:discipline])
        year ~ ChooseUniformly(possibilities[:year])
        undergraduate ~ ChooseUniformly(possibilities[:undergraduate])
        graduate ~ ChooseUniformly(possibilities[:graduate])
        year ~ ChooseUniformly(possibilities[:year])
        totalenrollment_ay ~ ChooseUniformly(possibilities[:totalenrollment_ay])
        fte_ay ~ ChooseUniformly(possibilities[:fte_ay])
        year ~ ChooseUniformly(possibilities[:year])
        faculty ~ ChooseUniformly(possibilities[:faculty])
    end
end

query = @query Csu1Model.Obs [
    campuses_id campuses.id
    campuses_campus campuses.campus
    campuses_location campuses.location
    campuses_county campuses.county
    campuses_year campuses.year
    csu_fees_year year
    csu_fees_campus_fee campus_fee
    degrees_year year
    degrees degrees
    discipline_enrollments_discipline discipline
    discipline_enrollments_year year
    discipline_enrollments_undergraduate undergraduate
    discipline_enrollments_graduate graduate
    enrollments_year year
    enrollments_totalenrollment_ay totalenrollment_ay
    enrollments_fte_ay fte_ay
    faculty_year year
    faculty faculty
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
