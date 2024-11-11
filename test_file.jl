using PClean
using DataFrames: DataFrame
import CSV

# Load data
data = CSV.File(filepath) |> DataFrame

# Define PClean model
PClean.@model MyModel begin
    @class ClassName1 begin
        ...
    end

    ...
    
    @class ClassNameN begin
        ...
    end
end

# Align column names of CSV with variables in the model.
# Format is ColumnName CleanVariable DirtyVariable, or, if
# there is no corruption for a certain variable, one can omit
# the DirtyVariable.
query = @query MyModel.ClassNameN [
  HospitalName hosp.name             observed_hosp_name
  Condition    metric.condition.desc observed_condition
  ...
]

# Configure observed dataset
observations = [ObservedDataset(query, data)]

# Configuration
config = PClean.InferenceConfig(1, 2; use_mh_instead_of_pg=true)

# SMC initialization
state = initialize_trace(observations, config)

# Rejuvenation sweeps
run_inference!(state, config)

# Evaluate accuracy, if ground truth is available
ground_truth = CSV.File(filepath) |> CSV.DataFrame
results = evaluate_accuracy(data, ground_truth, state, query)

# Can print results.f1, results.precision, results.accuracy, etc.
println(results)

# Even without ground truth, can save the entire latent database to CSV files:
PClean.save_results(dir, dataset_name, state, observations)
