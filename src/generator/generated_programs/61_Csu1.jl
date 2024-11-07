using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("campuses_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("campuses_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "campus"], Any[0, "location"], Any[0, "county"], Any[0, "year"], Any[1, "campus"], Any[1, "year"], Any[1, "campus fee"], Any[2, "year"], Any[2, "campus"], Any[2, "degrees"], Any[3, "campus"], Any[3, "discipline"], Any[3, "year"], Any[3, "undergraduate"], Any[3, "graduate"], Any[4, "campus"], Any[4, "year"], Any[4, "totalenrollment ay"], Any[4, "fte ay"], Any[5, "campus"], Any[5, "year"], Any[5, "faculty"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "campus"], Any[0, "location"], Any[0, "county"], Any[0, "year"], Any[1, "campus"], Any[1, "year"], Any[1, "campus fee"], Any[2, "year"], Any[2, "campus"], Any[2, "degrees"], Any[3, "campus"], Any[3, "discipline"], Any[3, "year"], Any[3, "undergraduate"], Any[3, "graduate"], Any[4, "campus"], Any[4, "year"], Any[4, "totalenrollment ay"], Any[4, "fte ay"], Any[5, "campus"], Any[5, "year"], Any[5, "faculty"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Csu_Fees begin
        campus ~ ChooseUniformly(possibilities[:campus])
        year ~ ChooseUniformly(possibilities[:year])
        campus_fee ~ ChooseUniformly(possibilities[:campus_fee])
    end

    @class Degrees begin
        year ~ ChooseUniformly(possibilities[:year])
        campus ~ ChooseUniformly(possibilities[:campus])
        degrees ~ ChooseUniformly(possibilities[:degrees])
    end

    @class Discipline_Enrollments begin
        campus ~ ChooseUniformly(possibilities[:campus])
        discipline ~ ChooseUniformly(possibilities[:discipline])
        year ~ ChooseUniformly(possibilities[:year])
        undergraduate ~ ChooseUniformly(possibilities[:undergraduate])
        graduate ~ ChooseUniformly(possibilities[:graduate])
    end

    @class Enrollments begin
        campus ~ ChooseUniformly(possibilities[:campus])
        year ~ ChooseUniformly(possibilities[:year])
        totalenrollment_ay ~ ChooseUniformly(possibilities[:totalenrollment_ay])
        fte_ay ~ ChooseUniformly(possibilities[:fte_ay])
    end

    @class Faculty begin
        campus ~ ChooseUniformly(possibilities[:campus])
        year ~ ChooseUniformly(possibilities[:year])
        faculty ~ ChooseUniformly(possibilities[:faculty])
    end

    @class Obs begin
        campuses ~ Campuses
        csu_Fees ~ Csu_Fees
        degrees ~ Degrees
        discipline_Enrollments ~ Discipline_Enrollments
        enrollments ~ Enrollments
        faculty ~ Faculty
    end
end

query = @query Csu1Model.Obs [
    campuses_id campuses.id
    campuses_campus campuses.campus
    campuses_location campuses.location
    campuses_county campuses.county
    campuses_year campuses.year
    csu_fees_year csu_Fees.year
    csu_fees_campus_fee csu_Fees.campus_fee
    degrees_year degrees.year
    degrees degrees.degrees
    discipline_enrollments_discipline discipline_Enrollments.discipline
    discipline_enrollments_year discipline_Enrollments.year
    discipline_enrollments_undergraduate discipline_Enrollments.undergraduate
    discipline_enrollments_graduate discipline_Enrollments.graduate
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
