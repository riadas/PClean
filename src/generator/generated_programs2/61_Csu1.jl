using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("campuses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("campuses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "id"], Any[0, "campus"], Any[0, "location"], Any[0, "county"], Any[0, "year"], Any[1, "campus"], Any[1, "year"], Any[1, "campus fee"], Any[2, "year"], Any[2, "campus"], Any[2, "degrees"], Any[3, "campus"], Any[3, "discipline"], Any[3, "year"], Any[3, "undergraduate"], Any[3, "graduate"], Any[4, "campus"], Any[4, "year"], Any[4, "totalenrollment ay"], Any[4, "fte ay"], Any[5, "campus"], Any[5, "year"], Any[5, "faculty"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[6, 1], Any[10, 1], Any[12, 1], Any[17, 1], Any[21, 1]])
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







PClean.@model Csu1Model begin
    @class Campuses begin
        campus ~ ChooseUniformly(possibilities[:campus])
        location ~ ChooseUniformly(possibilities[:location])
        county ~ ChooseUniformly(possibilities[:county])
        year ~ ChooseUniformly(possibilities[:year])
    end

    @class Csu_fees begin
        campuses ~ Campuses
        year ~ ChooseUniformly(possibilities[:year])
        campus_fee ~ ChooseUniformly(possibilities[:campus_fee])
    end

    @class Degrees begin
        year ~ ChooseUniformly(possibilities[:year])
        campuses ~ Campuses
        degrees ~ ChooseUniformly(possibilities[:degrees])
    end

    @class Discipline_enrollments begin
        campuses ~ Campuses
        discipline ~ ChooseUniformly(possibilities[:discipline])
        year ~ ChooseUniformly(possibilities[:year])
        undergraduate ~ ChooseUniformly(possibilities[:undergraduate])
        graduate ~ ChooseUniformly(possibilities[:graduate])
    end

    @class Enrollments begin
        campuses ~ Campuses
        year ~ ChooseUniformly(possibilities[:year])
        totalenrollment_ay ~ ChooseUniformly(possibilities[:totalenrollment_ay])
        fte_ay ~ ChooseUniformly(possibilities[:fte_ay])
    end

    @class Faculty begin
        campuses ~ Campuses
        year ~ ChooseUniformly(possibilities[:year])
        faculty ~ ChooseUniformly(possibilities[:faculty])
    end

    @class Obs begin
        csu_fees ~ Csu_fees
        degrees ~ Degrees
        discipline_enrollments ~ Discipline_enrollments
        enrollments ~ Enrollments
        faculty ~ Faculty
    end
end

query = @query Csu1Model.Obs [
    campuses_campus csu_fees.campuses.campus
    campuses_location csu_fees.campuses.location
    campuses_county csu_fees.campuses.county
    campuses_year csu_fees.campuses.year
    csu_fees_year csu_fees.year
    csu_fees_campus_fee csu_fees.campus_fee
    degrees_year degrees.year
    degrees degrees.degrees
    discipline_enrollments_discipline discipline_enrollments.discipline
    discipline_enrollments_year discipline_enrollments.year
    discipline_enrollments_undergraduate discipline_enrollments.undergraduate
    discipline_enrollments_graduate discipline_enrollments.graduate
    enrollments_year enrollments.year
    enrollments_totalenrollment_ay enrollments.totalenrollment_ay
    enrollments_fte_ay enrollments.fte_ay
    faculty_year faculty.year
    faculty faculty.faculty
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
