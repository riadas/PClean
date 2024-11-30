using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("repair_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("repair_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = length(dirty_table)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "repair id"], Any[0, "name"], Any[0, "launch date"], Any[0, "notes"], Any[1, "machine id"], Any[1, "making year"], Any[1, "class"], Any[1, "team"], Any[1, "machine series"], Any[1, "value points"], Any[1, "quality rank"], Any[2, "technician id"], Any[2, "name"], Any[2, "team"], Any[2, "starting year"], Any[2, "age"], Any[3, "technician id"], Any[3, "repair id"], Any[3, "machine id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "repair id"], Any[0, "name"], Any[0, "launch date"], Any[0, "notes"], Any[1, "machine id"], Any[1, "making year"], Any[1, "class"], Any[1, "team"], Any[1, "machine series"], Any[1, "value points"], Any[1, "quality rank"], Any[2, "technician id"], Any[2, "name"], Any[2, "team"], Any[2, "starting year"], Any[2, "age"], Any[3, "technician id"], Any[3, "repair id"], Any[3, "machine id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["machine id", "repair id", "technician id"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "name"], Any[0, "launch date"], Any[0, "notes"], Any[1, "making year"], Any[1, "class"], Any[1, "team"], Any[1, "machine series"], Any[1, "value points"], Any[1, "quality rank"], Any[2, "name"], Any[2, "team"], Any[2, "starting year"], Any[2, "age"]]
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





PClean.@model MachineRepairModel begin
    @class Repair begin
        repair_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        launch_date ~ ChooseUniformly(possibilities[:launch_date])
        notes ~ ChooseUniformly(possibilities[:notes])
    end

    @class Machine begin
        machine_id ~ Unmodeled()
        making_year ~ ChooseUniformly(possibilities[:making_year])
        class ~ ChooseUniformly(possibilities[:class])
        team ~ ChooseUniformly(possibilities[:team])
        machine_series ~ ChooseUniformly(possibilities[:machine_series])
        value_points ~ ChooseUniformly(possibilities[:value_points])
        quality_rank ~ ChooseUniformly(possibilities[:quality_rank])
    end

    @class Technician begin
        technician_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        team ~ ChooseUniformly(possibilities[:team])
        starting_year ~ ChooseUniformly(possibilities[:starting_year])
        age ~ ChooseUniformly(possibilities[:age])
    end

    @class Obs begin
        repair ~ Repair
        machine ~ Machine
        technician ~ Technician
    end
end

query = @query MachineRepairModel.Obs [
    repair_id repair.repair_id
    repair_name repair.name
    repair_launch_date repair.launch_date
    repair_notes repair.notes
    machine_id machine.machine_id
    machine_making_year machine.making_year
    machine_class machine.class
    machine_team machine.team
    machine_series machine.machine_series
    machine_value_points machine.value_points
    machine_quality_rank machine.quality_rank
    technician_id technician.technician_id
    technician_name technician.name
    technician_team technician.team
    technician_starting_year technician.starting_year
    technician_age technician.age
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

accuracy = evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query)
println(accuracy)
