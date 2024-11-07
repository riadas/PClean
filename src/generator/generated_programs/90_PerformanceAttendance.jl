using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("member_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("member_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "member id"], Any[0, "name"], Any[0, "nationality"], Any[0, "role"], Any[1, "performance id"], Any[1, "date"], Any[1, "host"], Any[1, "location"], Any[1, "attendance"], Any[2, "member id"], Any[2, "performance id"], Any[2, "num of pieces"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "member id"], Any[0, "name"], Any[0, "nationality"], Any[0, "role"], Any[1, "performance id"], Any[1, "date"], Any[1, "host"], Any[1, "location"], Any[1, "attendance"], Any[2, "member id"], Any[2, "performance id"], Any[2, "num of pieces"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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

    @class Member_Attendance begin
        member_id ~ Unmodeled()
        performance_id ~ ChooseUniformly(possibilities[:performance_id])
        num_of_pieces ~ ChooseUniformly(possibilities[:num_of_pieces])
    end

    @class Obs begin
        member ~ Member
        performance ~ Performance
        member_Attendance ~ Member_Attendance
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
    member_attendance_num_of_pieces member_Attendance.num_of_pieces
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
