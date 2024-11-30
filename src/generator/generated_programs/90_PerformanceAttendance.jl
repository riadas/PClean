using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("member_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("member_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "member id"], Any[0, "name"], Any[0, "nationality"], Any[0, "role"], Any[1, "performance id"], Any[1, "date"], Any[1, "host"], Any[1, "location"], Any[1, "attendance"], Any[2, "member id"], Any[2, "performance id"], Any[2, "num of pieces"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "member id"], Any[0, "name"], Any[0, "nationality"], Any[0, "role"], Any[1, "performance id"], Any[1, "date"], Any[1, "host"], Any[1, "location"], Any[1, "attendance"], Any[2, "member id"], Any[2, "performance id"], Any[2, "num of pieces"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["performance id", "member id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "nationality"], Any[0, "role"], Any[1, "date"], Any[1, "host"], Any[1, "location"], Any[1, "attendance"], Any[2, "num of pieces"]]
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





PClean.@model PerformanceAttendanceModel begin
    @class Member begin
        member_id ~ ChooseUniformly(possibilities[:member_id])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        role ~ ChooseUniformly(possibilities[:role])
    end

    @class Performance begin
        performance_id ~ Unmodeled()
        date ~ ChooseUniformly(possibilities[:date])
        host ~ ChooseUniformly(possibilities[:host])
        location ~ ChooseUniformly(possibilities[:location])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end

    @class Obs begin
        member ~ Member
        performance ~ Performance
        num_of_pieces ~ ChooseUniformly(possibilities[:num_of_pieces])
    end
end

query = @query PerformanceAttendanceModel.Obs [
    member_id member.member_id
    member_name member.name
    member_nationality member.nationality
    member_role member.role
    performance_id performance.performance_id
    performance_date performance.date
    performance_host performance.host
    performance_location performance.location
    performance_attendance performance.attendance
    member_attendance_num_of_pieces num_of_pieces
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
