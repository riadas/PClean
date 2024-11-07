using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("problem category codes_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("problem category codes_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "problem category code"], Any[0, "problem category description"], Any[1, "problem log id"], Any[1, "assigned to staff id"], Any[1, "problem id"], Any[1, "problem category code"], Any[1, "problem status code"], Any[1, "log entry date"], Any[1, "log entry description"], Any[1, "log entry fix"], Any[1, "other log details"], Any[2, "problem status code"], Any[2, "problem status description"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "staff id"], Any[4, "staff first name"], Any[4, "staff last name"], Any[4, "other staff details"], Any[5, "problem id"], Any[5, "product id"], Any[5, "closure authorised by staff id"], Any[5, "reported by staff id"], Any[5, "date problem reported"], Any[5, "date problem closed"], Any[5, "problem description"], Any[5, "other problem details"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "problem category code"], Any[0, "problem category description"], Any[1, "problem log id"], Any[1, "assigned to staff id"], Any[1, "problem id"], Any[1, "problem category code"], Any[1, "problem status code"], Any[1, "log entry date"], Any[1, "log entry description"], Any[1, "log entry fix"], Any[1, "other log details"], Any[2, "problem status code"], Any[2, "problem status description"], Any[3, "product id"], Any[3, "product name"], Any[3, "product details"], Any[4, "staff id"], Any[4, "staff first name"], Any[4, "staff last name"], Any[4, "other staff details"], Any[5, "problem id"], Any[5, "product id"], Any[5, "closure authorised by staff id"], Any[5, "reported by staff id"], Any[5, "date problem reported"], Any[5, "date problem closed"], Any[5, "problem description"], Any[5, "other problem details"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model TrackingSoftwareProblemsModel begin
    @class Problem_Category_Codes begin
        problem_category_code ~ ChooseUniformly(possibilities[:problem_category_code])
        problem_category_description ~ ChooseUniformly(possibilities[:problem_category_description])
    end

    @class Problem_Log begin
        problem_log_id ~ Unmodeled()
        assigned_to_staff_id ~ ChooseUniformly(possibilities[:assigned_to_staff_id])
        problem_id ~ ChooseUniformly(possibilities[:problem_id])
        problem_category_code ~ ChooseUniformly(possibilities[:problem_category_code])
        problem_status_code ~ ChooseUniformly(possibilities[:problem_status_code])
        log_entry_date ~ TimePrior(possibilities[:log_entry_date])
        log_entry_description ~ ChooseUniformly(possibilities[:log_entry_description])
        log_entry_fix ~ ChooseUniformly(possibilities[:log_entry_fix])
        other_log_details ~ ChooseUniformly(possibilities[:other_log_details])
    end

    @class Problem_Status_Codes begin
        problem_status_code ~ ChooseUniformly(possibilities[:problem_status_code])
        problem_status_description ~ ChooseUniformly(possibilities[:problem_status_description])
    end

    @class Product begin
        product_id ~ Unmodeled()
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_details ~ ChooseUniformly(possibilities[:product_details])
    end

    @class Staff begin
        staff_id ~ Unmodeled()
        staff_first_name ~ ChooseUniformly(possibilities[:staff_first_name])
        staff_last_name ~ ChooseUniformly(possibilities[:staff_last_name])
        other_staff_details ~ ChooseUniformly(possibilities[:other_staff_details])
    end

    @class Problems begin
        problem_id ~ Unmodeled()
        product_id ~ ChooseUniformly(possibilities[:product_id])
        closure_authorised_by_staff_id ~ ChooseUniformly(possibilities[:closure_authorised_by_staff_id])
        reported_by_staff_id ~ ChooseUniformly(possibilities[:reported_by_staff_id])
        date_problem_reported ~ TimePrior(possibilities[:date_problem_reported])
        date_problem_closed ~ TimePrior(possibilities[:date_problem_closed])
        problem_description ~ ChooseUniformly(possibilities[:problem_description])
        other_problem_details ~ ChooseUniformly(possibilities[:other_problem_details])
    end

    @class Obs begin
        problem_Category_Codes ~ Problem_Category_Codes
        problem_Log ~ Problem_Log
        problem_Status_Codes ~ Problem_Status_Codes
        product ~ Product
        staff ~ Staff
        problems ~ Problems
    end
end

query = @query TrackingSoftwareProblemsModel.Obs [
    problem_category_codes_problem_category_code problem_Category_Codes.problem_category_code
    problem_category_codes_problem_category_description problem_Category_Codes.problem_category_description
    problem_log_id problem_Log.problem_log_id
    problem_log_log_entry_date problem_Log.log_entry_date
    problem_log_log_entry_description problem_Log.log_entry_description
    problem_log_log_entry_fix problem_Log.log_entry_fix
    problem_log_other_log_details problem_Log.other_log_details
    problem_status_codes_problem_status_code problem_Status_Codes.problem_status_code
    problem_status_codes_problem_status_description problem_Status_Codes.problem_status_description
    product_id product.product_id
    product_name product.product_name
    product_details product.product_details
    staff_id staff.staff_id
    staff_first_name staff.staff_first_name
    staff_last_name staff.staff_last_name
    other_staff_details staff.other_staff_details
    problems_problem_id problems.problem_id
    problems_date_problem_reported problems.date_problem_reported
    problems_date_problem_closed problems.date_problem_closed
    problems_problem_description problems.problem_description
    problems_other_problem_details problems.other_problem_details
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
