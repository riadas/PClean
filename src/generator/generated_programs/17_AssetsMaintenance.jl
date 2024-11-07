using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("third party companies_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("third party companies_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "company id"], Any[0, "company type"], Any[0, "company name"], Any[0, "company address"], Any[0, "other company details"], Any[1, "maintenance contract id"], Any[1, "maintenance contract company id"], Any[1, "contract start date"], Any[1, "contract end date"], Any[1, "other contract details"], Any[2, "part id"], Any[2, "part name"], Any[2, "chargeable yn"], Any[2, "chargeable amount"], Any[2, "other part details"], Any[3, "skill id"], Any[3, "skill code"], Any[3, "skill description"], Any[4, "staff id"], Any[4, "staff name"], Any[4, "gender"], Any[4, "other staff details"], Any[5, "asset id"], Any[5, "maintenance contract id"], Any[5, "supplier company id"], Any[5, "asset details"], Any[5, "asset make"], Any[5, "asset model"], Any[5, "asset acquired date"], Any[5, "asset disposed date"], Any[5, "other asset details"], Any[6, "asset id"], Any[6, "part id"], Any[7, "engineer id"], Any[7, "company id"], Any[7, "first name"], Any[7, "last name"], Any[7, "other details"], Any[8, "engineer id"], Any[8, "skill id"], Any[9, "fault log entry id"], Any[9, "asset id"], Any[9, "recorded by staff id"], Any[9, "fault log entry datetime"], Any[9, "fault description"], Any[9, "other fault details"], Any[10, "engineer visit id"], Any[10, "contact staff id"], Any[10, "engineer id"], Any[10, "fault log entry id"], Any[10, "fault status"], Any[10, "visit start datetime"], Any[10, "visit end datetime"], Any[10, "other visit details"], Any[11, "part fault id"], Any[11, "part id"], Any[11, "fault short name"], Any[11, "fault description"], Any[11, "other fault details"], Any[12, "fault log entry id"], Any[12, "part fault id"], Any[12, "fault status"], Any[13, "part fault id"], Any[13, "skill id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "company id"], Any[0, "company type"], Any[0, "company name"], Any[0, "company address"], Any[0, "other company details"], Any[1, "maintenance contract id"], Any[1, "maintenance contract company id"], Any[1, "contract start date"], Any[1, "contract end date"], Any[1, "other contract details"], Any[2, "part id"], Any[2, "part name"], Any[2, "chargeable yn"], Any[2, "chargeable amount"], Any[2, "other part details"], Any[3, "skill id"], Any[3, "skill code"], Any[3, "skill description"], Any[4, "staff id"], Any[4, "staff name"], Any[4, "gender"], Any[4, "other staff details"], Any[5, "asset id"], Any[5, "maintenance contract id"], Any[5, "supplier company id"], Any[5, "asset details"], Any[5, "asset make"], Any[5, "asset model"], Any[5, "asset acquired date"], Any[5, "asset disposed date"], Any[5, "other asset details"], Any[6, "asset id"], Any[6, "part id"], Any[7, "engineer id"], Any[7, "company id"], Any[7, "first name"], Any[7, "last name"], Any[7, "other details"], Any[8, "engineer id"], Any[8, "skill id"], Any[9, "fault log entry id"], Any[9, "asset id"], Any[9, "recorded by staff id"], Any[9, "fault log entry datetime"], Any[9, "fault description"], Any[9, "other fault details"], Any[10, "engineer visit id"], Any[10, "contact staff id"], Any[10, "engineer id"], Any[10, "fault log entry id"], Any[10, "fault status"], Any[10, "visit start datetime"], Any[10, "visit end datetime"], Any[10, "other visit details"], Any[11, "part fault id"], Any[11, "part id"], Any[11, "fault short name"], Any[11, "fault description"], Any[11, "other fault details"], Any[12, "fault log entry id"], Any[12, "part fault id"], Any[12, "fault status"], Any[13, "part fault id"], Any[13, "skill id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model AssetsMaintenanceModel begin
    @class Third_Party_Companies begin
        company_id ~ Unmodeled()
        company_type ~ ChooseUniformly(possibilities[:company_type])
        company_name ~ ChooseUniformly(possibilities[:company_name])
        company_address ~ ChooseUniformly(possibilities[:company_address])
        other_company_details ~ ChooseUniformly(possibilities[:other_company_details])
    end

    @class Maintenance_Contracts begin
        maintenance_contract_id ~ Unmodeled()
        maintenance_contract_company_id ~ ChooseUniformly(possibilities[:maintenance_contract_company_id])
        contract_start_date ~ TimePrior(possibilities[:contract_start_date])
        contract_end_date ~ TimePrior(possibilities[:contract_end_date])
        other_contract_details ~ ChooseUniformly(possibilities[:other_contract_details])
    end

    @class Parts begin
        part_id ~ Unmodeled()
        part_name ~ ChooseUniformly(possibilities[:part_name])
        chargeable_yn ~ ChooseUniformly(possibilities[:chargeable_yn])
        chargeable_amount ~ ChooseUniformly(possibilities[:chargeable_amount])
        other_part_details ~ ChooseUniformly(possibilities[:other_part_details])
    end

    @class Skills begin
        skill_id ~ Unmodeled()
        skill_code ~ ChooseUniformly(possibilities[:skill_code])
        skill_description ~ ChooseUniformly(possibilities[:skill_description])
    end

    @class Staff begin
        staff_id ~ Unmodeled()
        staff_name ~ ChooseUniformly(possibilities[:staff_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        other_staff_details ~ ChooseUniformly(possibilities[:other_staff_details])
    end

    @class Assets begin
        asset_id ~ Unmodeled()
        maintenance_contract_id ~ ChooseUniformly(possibilities[:maintenance_contract_id])
        supplier_company_id ~ ChooseUniformly(possibilities[:supplier_company_id])
        asset_details ~ ChooseUniformly(possibilities[:asset_details])
        asset_make ~ ChooseUniformly(possibilities[:asset_make])
        asset_model ~ ChooseUniformly(possibilities[:asset_model])
        asset_acquired_date ~ TimePrior(possibilities[:asset_acquired_date])
        asset_disposed_date ~ TimePrior(possibilities[:asset_disposed_date])
        other_asset_details ~ ChooseUniformly(possibilities[:other_asset_details])
    end

    @class Asset_Parts begin
        asset_id ~ Unmodeled()
        part_id ~ ChooseUniformly(possibilities[:part_id])
    end

    @class Maintenance_Engineers begin
        engineer_id ~ Unmodeled()
        company_id ~ ChooseUniformly(possibilities[:company_id])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Engineer_Skills begin
        engineer_id ~ Unmodeled()
        skill_id ~ ChooseUniformly(possibilities[:skill_id])
    end

    @class Fault_Log begin
        fault_log_entry_id ~ Unmodeled()
        asset_id ~ ChooseUniformly(possibilities[:asset_id])
        recorded_by_staff_id ~ ChooseUniformly(possibilities[:recorded_by_staff_id])
        fault_log_entry_datetime ~ TimePrior(possibilities[:fault_log_entry_datetime])
        fault_description ~ ChooseUniformly(possibilities[:fault_description])
        other_fault_details ~ ChooseUniformly(possibilities[:other_fault_details])
    end

    @class Engineer_Visits begin
        engineer_visit_id ~ Unmodeled()
        contact_staff_id ~ ChooseUniformly(possibilities[:contact_staff_id])
        engineer_id ~ ChooseUniformly(possibilities[:engineer_id])
        fault_log_entry_id ~ ChooseUniformly(possibilities[:fault_log_entry_id])
        fault_status ~ ChooseUniformly(possibilities[:fault_status])
        visit_start_datetime ~ TimePrior(possibilities[:visit_start_datetime])
        visit_end_datetime ~ TimePrior(possibilities[:visit_end_datetime])
        other_visit_details ~ ChooseUniformly(possibilities[:other_visit_details])
    end

    @class Part_Faults begin
        part_fault_id ~ Unmodeled()
        part_id ~ ChooseUniformly(possibilities[:part_id])
        fault_short_name ~ ChooseUniformly(possibilities[:fault_short_name])
        fault_description ~ ChooseUniformly(possibilities[:fault_description])
        other_fault_details ~ ChooseUniformly(possibilities[:other_fault_details])
    end

    @class Fault_Log_Parts begin
        fault_log_entry_id ~ Unmodeled()
        part_fault_id ~ ChooseUniformly(possibilities[:part_fault_id])
        fault_status ~ ChooseUniformly(possibilities[:fault_status])
    end

    @class Skills_Required_To_Fix begin
        part_fault_id ~ Unmodeled()
        skill_id ~ ChooseUniformly(possibilities[:skill_id])
    end

    @class Obs begin
        third_Party_Companies ~ Third_Party_Companies
        maintenance_Contracts ~ Maintenance_Contracts
        parts ~ Parts
        skills ~ Skills
        staff ~ Staff
        assets ~ Assets
        asset_Parts ~ Asset_Parts
        maintenance_Engineers ~ Maintenance_Engineers
        engineer_Skills ~ Engineer_Skills
        fault_Log ~ Fault_Log
        engineer_Visits ~ Engineer_Visits
        part_Faults ~ Part_Faults
        fault_Log_Parts ~ Fault_Log_Parts
        skills_Required_To_Fix ~ Skills_Required_To_Fix
    end
end

query = @query AssetsMaintenanceModel.Obs [
    third_party_companies_company_id third_Party_Companies.company_id
    third_party_companies_company_type third_Party_Companies.company_type
    third_party_companies_company_name third_Party_Companies.company_name
    third_party_companies_company_address third_Party_Companies.company_address
    third_party_companies_other_company_details third_Party_Companies.other_company_details
    maintenance_contracts_maintenance_contract_id maintenance_Contracts.maintenance_contract_id
    maintenance_contracts_contract_start_date maintenance_Contracts.contract_start_date
    maintenance_contracts_contract_end_date maintenance_Contracts.contract_end_date
    maintenance_contracts_other_contract_details maintenance_Contracts.other_contract_details
    parts_part_id parts.part_id
    parts_part_name parts.part_name
    parts_chargeable_yn parts.chargeable_yn
    parts_chargeable_amount parts.chargeable_amount
    parts_other_part_details parts.other_part_details
    skills_skill_id skills.skill_id
    skills_skill_code skills.skill_code
    skills_skill_description skills.skill_description
    staff_id staff.staff_id
    staff_name staff.staff_name
    staff_gender staff.gender
    other_staff_details staff.other_staff_details
    assets_asset_id assets.asset_id
    assets_asset_details assets.asset_details
    assets_asset_make assets.asset_make
    assets_asset_model assets.asset_model
    assets_asset_acquired_date assets.asset_acquired_date
    assets_asset_disposed_date assets.asset_disposed_date
    assets_other_asset_details assets.other_asset_details
    maintenance_engineers_engineer_id maintenance_Engineers.engineer_id
    maintenance_engineers_first_name maintenance_Engineers.first_name
    maintenance_engineers_last_name maintenance_Engineers.last_name
    maintenance_engineers_other_details maintenance_Engineers.other_details
    fault_log_entry_id fault_Log.fault_log_entry_id
    fault_log_entry_datetime fault_Log.fault_log_entry_datetime
    fault_log_fault_description fault_Log.fault_description
    fault_log_other_fault_details fault_Log.other_fault_details
    engineer_visits_engineer_visit_id engineer_Visits.engineer_visit_id
    engineer_visits_fault_status engineer_Visits.fault_status
    engineer_visits_visit_start_datetime engineer_Visits.visit_start_datetime
    engineer_visits_visit_end_datetime engineer_Visits.visit_end_datetime
    engineer_visits_other_visit_details engineer_Visits.other_visit_details
    part_faults_part_fault_id part_Faults.part_fault_id
    part_faults_fault_short_name part_Faults.fault_short_name
    part_faults_fault_description part_Faults.fault_description
    part_faults_other_fault_details part_Faults.other_fault_details
    fault_log_parts_fault_status fault_Log_Parts.fault_status
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
