using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("building_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("building_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "building id"], Any[0, "name"], Any[0, "street address"], Any[0, "years as tallest"], Any[0, "height feet"], Any[0, "floors"], Any[1, "institution id"], Any[1, "institution"], Any[1, "location"], Any[1, "founded"], Any[1, "type"], Any[1, "enrollment"], Any[1, "team"], Any[1, "primary conference"], Any[1, "building id"], Any[2, "common name"], Any[2, "protein name"], Any[2, "divergence from human lineage"], Any[2, "accession number"], Any[2, "sequence length"], Any[2, "sequence identity to human protein"], Any[2, "institution id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "building id"], Any[0, "name"], Any[0, "street address"], Any[0, "years as tallest"], Any[0, "height feet"], Any[0, "floors"], Any[1, "institution id"], Any[1, "institution"], Any[1, "location"], Any[1, "founded"], Any[1, "type"], Any[1, "enrollment"], Any[1, "team"], Any[1, "primary conference"], Any[1, "building id"], Any[2, "common name"], Any[2, "protein name"], Any[2, "divergence from human lineage"], Any[2, "accession number"], Any[2, "sequence length"], Any[2, "sequence identity to human protein"], Any[2, "institution id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ProteinInstituteModel begin
    @class Building begin
        building_id ~ ChooseUniformly(possibilities[:building_id])
        name ~ ChooseUniformly(possibilities[:name])
        street_address ~ ChooseUniformly(possibilities[:street_address])
        years_as_tallest ~ ChooseUniformly(possibilities[:years_as_tallest])
        height_feet ~ ChooseUniformly(possibilities[:height_feet])
        floors ~ ChooseUniformly(possibilities[:floors])
    end

    @class Institution begin
        institution_id ~ ChooseUniformly(possibilities[:institution_id])
        institution ~ ChooseUniformly(possibilities[:institution])
        location ~ ChooseUniformly(possibilities[:location])
        founded ~ ChooseUniformly(possibilities[:founded])
        type ~ ChooseUniformly(possibilities[:type])
        enrollment ~ ChooseUniformly(possibilities[:enrollment])
        team ~ ChooseUniformly(possibilities[:team])
        primary_conference ~ ChooseUniformly(possibilities[:primary_conference])
        building_id ~ ChooseUniformly(possibilities[:building_id])
    end

    @class Protein begin
        common_name ~ ChooseUniformly(possibilities[:common_name])
        protein_name ~ ChooseUniformly(possibilities[:protein_name])
        divergence_from_human_lineage ~ ChooseUniformly(possibilities[:divergence_from_human_lineage])
        accession_number ~ ChooseUniformly(possibilities[:accession_number])
        sequence_length ~ ChooseUniformly(possibilities[:sequence_length])
        sequence_identity_to_human_protein ~ ChooseUniformly(possibilities[:sequence_identity_to_human_protein])
        institution_id ~ ChooseUniformly(possibilities[:institution_id])
    end

    @class Obs begin
        building ~ Building
        institution ~ Institution
        protein ~ Protein
    end
end

query = @query ProteinInstituteModel.Obs [
    building_id building.building_id
    building_name building.name
    building_street_address building.street_address
    building_years_as_tallest building.years_as_tallest
    building_height_feet building.height_feet
    building_floors building.floors
    institution_id institution.institution_id
    institution institution.institution
    institution_location institution.location
    institution_founded institution.founded
    institution_type institution.type
    institution_enrollment institution.enrollment
    institution_team institution.team
    institution_primary_conference institution.primary_conference
    protein_common_name protein.common_name
    protein_name protein.protein_name
    protein_divergence_from_human_lineage protein.divergence_from_human_lineage
    protein_accession_number protein.accession_number
    protein_sequence_length protein.sequence_length
    sequence_identity_to_human_protein protein.sequence_identity_to_human_protein
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
