using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("school_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("school_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school name"], Any[0, "location"], Any[0, "mascot"], Any[0, "enrollment"], Any[0, "ihsaa class"], Any[0, "ihsaa football class"], Any[0, "county"], Any[1, "school id"], Any[1, "year"], Any[1, "budgeted"], Any[1, "total budget percent budgeted"], Any[1, "invested"], Any[1, "total budget percent invested"], Any[1, "budget invested percent"], Any[2, "endowment id"], Any[2, "school id"], Any[2, "donator name"], Any[2, "amount"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school name"], Any[0, "location"], Any[0, "mascot"], Any[0, "enrollment"], Any[0, "ihsaa class"], Any[0, "ihsaa football class"], Any[0, "county"], Any[1, "school id"], Any[1, "year"], Any[1, "budgeted"], Any[1, "total budget percent budgeted"], Any[1, "invested"], Any[1, "total budget percent invested"], Any[1, "budget invested percent"], Any[2, "endowment id"], Any[2, "school id"], Any[2, "donator name"], Any[2, "amount"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model SchoolFinanceModel begin
    @class School begin
        school_id ~ ChooseUniformly(possibilities[:school_id])
        school_name ~ ChooseUniformly(possibilities[:school_name])
        location ~ ChooseUniformly(possibilities[:location])
        mascot ~ ChooseUniformly(possibilities[:mascot])
        enrollment ~ ChooseUniformly(possibilities[:enrollment])
        ihsaa_class ~ ChooseUniformly(possibilities[:ihsaa_class])
        ihsaa_football_class ~ ChooseUniformly(possibilities[:ihsaa_football_class])
        county ~ ChooseUniformly(possibilities[:county])
    end

    @class Budget begin
        school_id ~ Unmodeled()
        year ~ ChooseUniformly(possibilities[:year])
        budgeted ~ ChooseUniformly(possibilities[:budgeted])
        total_budget_percent_budgeted ~ ChooseUniformly(possibilities[:total_budget_percent_budgeted])
        invested ~ ChooseUniformly(possibilities[:invested])
        total_budget_percent_invested ~ ChooseUniformly(possibilities[:total_budget_percent_invested])
        budget_invested_percent ~ ChooseUniformly(possibilities[:budget_invested_percent])
    end

    @class Endowment begin
        endowment_id ~ Unmodeled()
        school_id ~ ChooseUniformly(possibilities[:school_id])
        donator_name ~ ChooseUniformly(possibilities[:donator_name])
        amount ~ ChooseUniformly(possibilities[:amount])
    end

    @class Obs begin
        school ~ School
        budget ~ Budget
        endowment ~ Endowment
    end
end

query = @query SchoolFinanceModel.Obs [
    school_id school.school_id
    school_name school.school_name
    school_location school.location
    school_mascot school.mascot
    school_enrollment school.enrollment
    school_ihsaa_class school.ihsaa_class
    school_ihsaa_football_class school.ihsaa_football_class
    school_county school.county
    budget_year budget.year
    budgeted budget.budgeted
    total_budget_percent_budgeted budget.total_budget_percent_budgeted
    budget_invested budget.invested
    total_budget_percent_invested budget.total_budget_percent_invested
    budget_invested_percent budget.budget_invested_percent
    endowment_id endowment.endowment_id
    endowment_donator_name endowment.donator_name
    endowment_amount endowment.amount
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
