using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("problem_category_codes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("problem_category_codes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "problem category code"], Any[0, "problem category description"], Any[1, "problem log id"], Any[1, "assigned to staff id"], Any[1, "problem id"], Any[1, "problem category code"], Any[1, "problem status code"], Any[1, "log entry date"], Any[1, "log entry description"], Any[1, "log entry fix"], Any[1, "other log details"], Any[2, "problem status code"], Any[2, "problem status description"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "staff id"], Any[4, "staff first name"], Any[4, "staff last name"], Any[4, "other staff details"], Any[5, "problem id"], Any[5, "product id"], Any[5, "closure authorised by staff id"], Any[5, "reported by staff id"], Any[5, "date problem reported"], Any[5, "date problem closed"], Any[5, "problem description"], Any[5, "other problem details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "problem category code"], Any[0, "problem category description"], Any[1, "problem log id"], Any[1, "assigned to staff id"], Any[1, "problem id"], Any[1, "problem category code"], Any[1, "problem status code"], Any[1, "log entry date"], Any[1, "log entry description"], Any[1, "log entry fix"], Any[1, "other log details"], Any[2, "problem status code"], Any[2, "problem status description"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "staff id"], Any[4, "staff first name"], Any[4, "staff last name"], Any[4, "other staff details"], Any[5, "problem id"], Any[5, "product id"], Any[5, "closure authorised by staff id"], Any[5, "reported by staff id"], Any[5, "date problem reported"], Any[5, "date problem closed"], Any[5, "problem description"], Any[5, "other problem details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "problem category code"], Any[0, "problem category description"], Any[1, "problem log id"], Any[1, "assigned to staff id"], Any[1, "problem id"], Any[1, "problem category code"], Any[1, "problem status code"], Any[1, "log entry date"], Any[1, "log entry description"], Any[1, "log entry fix"], Any[1, "other log details"], Any[2, "problem status code"], Any[2, "problem status description"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "staff id"], Any[4, "staff first name"], Any[4, "staff last name"], Any[4, "other staff details"], Any[5, "problem id"], Any[5, "product id"], Any[5, "closure authorised by staff id"], Any[5, "reported by staff id"], Any[5, "date problem reported"], Any[5, "date problem closed"], Any[5, "problem description"], Any[5, "other problem details"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "problem category code"], Any[0, "problem category description"], Any[1, "problem log id"], Any[1, "assigned to staff id"], Any[1, "problem id"], Any[1, "problem category code"], Any[1, "problem status code"], Any[1, "log entry date"], Any[1, "log entry description"], Any[1, "log entry fix"], Any[1, "other log details"], Any[2, "problem status code"], Any[2, "problem status description"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "staff id"], Any[4, "staff first name"], Any[4, "staff last name"], Any[4, "other staff details"], Any[5, "problem id"], Any[5, "product id"], Any[5, "closure authorised by staff id"], Any[5, "reported by staff id"], Any[5, "date problem reported"], Any[5, "date problem closed"], Any[5, "problem description"], Any[5, "other problem details"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "problem category code"], Any[0, "problem category description"], Any[1, "problem log id"], Any[1, "assigned to staff id"], Any[1, "problem id"], Any[1, "problem category code"], Any[1, "problem status code"], Any[1, "log entry date"], Any[1, "log entry description"], Any[1, "log entry fix"], Any[1, "other log details"], Any[2, "problem status code"], Any[2, "problem status description"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "staff id"], Any[4, "staff first name"], Any[4, "staff last name"], Any[4, "other staff details"], Any[5, "problem id"], Any[5, "product id"], Any[5, "closure authorised by staff id"], Any[5, "reported by staff id"], Any[5, "date problem reported"], Any[5, "date problem closed"], Any[5, "problem description"], Any[5, "other problem details"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 12], Any[5, 21], Any[4, 17], Any[6, 1], Any[24, 17], Any[22, 14], Any[23, 17]])
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







PClean.@model TrackingSoftwareProblemsModel begin
    @class Problem_category_codes begin
        problem_category_code ~ ChooseUniformly(possibilities[:problem_category_code])
        problem_category_description ~ ChooseUniformly(possibilities[:problem_category_description])
    end

    @class Problem_status_codes begin
        problem_status_code ~ ChooseUniformly(possibilities[:problem_status_code])
        problem_status_description ~ ChooseUniformly(possibilities[:problem_status_description])
    end

    @class Product begin
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_details ~ ChooseUniformly(possibilities[:product_details])
    end

    @class Staff begin
        staff_first_name ~ ChooseUniformly(possibilities[:staff_first_name])
        staff_last_name ~ ChooseUniformly(possibilities[:staff_last_name])
        other_staff_details ~ ChooseUniformly(possibilities[:other_staff_details])
    end

    @class Problems begin
        product ~ Product
        staff ~ Staff
        date_problem_reported ~ TimePrior(possibilities[:date_problem_reported])
        date_problem_closed ~ TimePrior(possibilities[:date_problem_closed])
        problem_description ~ ChooseUniformly(possibilities[:problem_description])
        other_problem_details ~ ChooseUniformly(possibilities[:other_problem_details])
    end

    @class Problem_log begin
        staff ~ Staff
        problems ~ Problems
        problem_category_codes ~ Problem_category_codes
        problem_status_codes ~ Problem_status_codes
        log_entry_date ~ TimePrior(possibilities[:log_entry_date])
        log_entry_description ~ ChooseUniformly(possibilities[:log_entry_description])
        log_entry_fix ~ ChooseUniformly(possibilities[:log_entry_fix])
        other_log_details ~ ChooseUniformly(possibilities[:other_log_details])
    end

    @class Obs begin
        problem_log ~ Problem_log
    end
end

query = @query TrackingSoftwareProblemsModel.Obs [
    problem_category_codes_problem_category_code problem_log.problem_category_codes.problem_category_code
    problem_category_codes_problem_category_description problem_log.problem_category_codes.problem_category_description
    problem_log_id problem_log.problem_log_id
    problem_log_log_entry_date problem_log.log_entry_date
    problem_log_log_entry_description problem_log.log_entry_description
    problem_log_log_entry_fix problem_log.log_entry_fix
    problem_log_other_log_details problem_log.other_log_details
    problem_status_codes_problem_status_code problem_log.problem_status_codes.problem_status_code
    problem_status_codes_problem_status_description problem_log.problem_status_codes.problem_status_description
    product_id problem_log.problems.product.product_id
    product_name problem_log.problems.product.product_name
    product_details problem_log.problems.product.product_details
    staff_id problem_log.staff.staff_id
    staff_first_name problem_log.staff.staff_first_name
    staff_last_name problem_log.staff.staff_last_name
    other_staff_details problem_log.staff.other_staff_details
    problems_problem_id problem_log.problems.problem_id
    problems_date_problem_reported problem_log.problems.date_problem_reported
    problems_date_problem_closed problem_log.problems.date_problem_closed
    problems_problem_description problem_log.problems.problem_description
    problems_other_problem_details problem_log.problems.other_problem_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
