using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("school_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("school_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school name"], Any[0, "location"], Any[0, "mascot"], Any[0, "enrollment"], Any[0, "ihsaa class"], Any[0, "ihsaa football class"], Any[0, "county"], Any[1, "school id"], Any[1, "year"], Any[1, "budgeted"], Any[1, "total budget percent budgeted"], Any[1, "invested"], Any[1, "total budget percent invested"], Any[1, "budget invested percent"], Any[2, "endowment id"], Any[2, "school id"], Any[2, "donator name"], Any[2, "amount"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "school id"], Any[0, "school name"], Any[0, "location"], Any[0, "mascot"], Any[0, "enrollment"], Any[0, "ihsaa class"], Any[0, "ihsaa football class"], Any[0, "county"], Any[1, "school id"], Any[1, "year"], Any[1, "budgeted"], Any[1, "total budget percent budgeted"], Any[1, "invested"], Any[1, "total budget percent invested"], Any[1, "budget invested percent"], Any[2, "endowment id"], Any[2, "school id"], Any[2, "donator name"], Any[2, "amount"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["school id", "school id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "school name"], Any[0, "location"], Any[0, "mascot"], Any[0, "enrollment"], Any[0, "ihsaa class"], Any[0, "ihsaa football class"], Any[0, "county"], Any[1, "year"], Any[1, "budgeted"], Any[1, "total budget percent budgeted"], Any[1, "invested"], Any[1, "total budget percent invested"], Any[1, "budget invested percent"], Any[2, "endowment id"], Any[2, "donator name"], Any[2, "amount"]]
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

    @class Obs begin
        school ~ School
        year ~ ChooseUniformly(possibilities[:year])
        budgeted ~ ChooseUniformly(possibilities[:budgeted])
        total_budget_percent_budgeted ~ ChooseUniformly(possibilities[:total_budget_percent_budgeted])
        invested ~ ChooseUniformly(possibilities[:invested])
        total_budget_percent_invested ~ ChooseUniformly(possibilities[:total_budget_percent_invested])
        budget_invested_percent ~ ChooseUniformly(possibilities[:budget_invested_percent])
        endowment_id ~ Unmodeled()
        donator_name ~ ChooseUniformly(possibilities[:donator_name])
        amount ~ ChooseUniformly(possibilities[:amount])
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
    budget_year year
    budgeted budgeted
    total_budget_percent_budgeted total_budget_percent_budgeted
    budget_invested invested
    total_budget_percent_invested total_budget_percent_invested
    budget_invested_percent budget_invested_percent
    endowment_id endowment_id
    endowment_donator_name donator_name
    endowment_amount amount
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
