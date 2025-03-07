using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("member_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("member_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
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
cols = Any[Any[-1, "*"], Any[0, "member id"], Any[0, "name"], Any[0, "nationality"], Any[0, "role"], Any[1, "performance id"], Any[1, "date"], Any[1, "host"], Any[1, "location"], Any[1, "attendance"], Any[2, "member id"], Any[2, "performance id"], Any[2, "num of pieces"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 5], Any[10, 1]])
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







PClean.@model PerformanceAttendanceModel begin
    @class Member begin
        member_id ~ ChooseUniformly(possibilities[:member_id])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        role ~ ChooseUniformly(possibilities[:role])
    end

    @class Performance begin
        date ~ ChooseUniformly(possibilities[:date])
        host ~ ChooseUniformly(possibilities[:host])
        location ~ ChooseUniformly(possibilities[:location])
        attendance ~ ChooseUniformly(possibilities[:attendance])
    end

    @class Member_attendance begin
        performance ~ Performance
        num_of_pieces ~ ChooseUniformly(possibilities[:num_of_pieces])
    end

    @class Obs begin
        member_attendance ~ Member_attendance
    end
end

query = @query PerformanceAttendanceModel.Obs [
    member_id member_attendance.member.member_id
    member_name member_attendance.member.name
    member_nationality member_attendance.member.nationality
    member_role member_attendance.member.role
    performance_id member_attendance.performance.performance_id
    performance_date member_attendance.performance.date
    performance_host member_attendance.performance.host
    performance_location member_attendance.performance.location
    performance_attendance member_attendance.performance.attendance
    member_attendance_num_of_pieces member_attendance.num_of_pieces
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
