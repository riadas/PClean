using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("third_party_companies_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("third_party_companies_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "company id"], Any[0, "company type"], Any[0, "company name"], Any[0, "company address"], Any[0, "other company details"], Any[1, "maintenance contract id"], Any[1, "maintenance contract company id"], Any[1, "contract start date"], Any[1, "contract end date"], Any[1, "other contract details"], Any[2, "part id"], Any[2, "part name"], Any[2, "chargeable yn"], Any[2, "chargeable amount"], Any[2, "other part details"], Any[3, "skill id"], Any[3, "skill code"], Any[3, "skill description"], Any[4, "staff id"], Any[4, "staff name"], Any[4, "gender"], Any[4, "other staff details"], Any[5, "asset id"], Any[5, "maintenance contract id"], Any[5, "supplier company id"], Any[5, "asset details"], Any[5, "asset make"], Any[5, "asset model"], Any[5, "asset acquired date"], Any[5, "asset disposed date"], Any[5, "other asset details"], Any[6, "asset id"], Any[6, "part id"], Any[7, "engineer id"], Any[7, "company id"], Any[7, "first name"], Any[7, "last name"], Any[7, "other details"], Any[8, "engineer id"], Any[8, "skill id"], Any[9, "fault log entry id"], Any[9, "asset id"], Any[9, "recorded by staff id"], Any[9, "fault log entry datetime"], Any[9, "fault description"], Any[9, "other fault details"], Any[10, "engineer visit id"], Any[10, "contact staff id"], Any[10, "engineer id"], Any[10, "fault log entry id"], Any[10, "fault status"], Any[10, "visit start datetime"], Any[10, "visit end datetime"], Any[10, "other visit details"], Any[11, "part fault id"], Any[11, "part id"], Any[11, "fault short name"], Any[11, "fault description"], Any[11, "other fault details"], Any[12, "fault log entry id"], Any[12, "part fault id"], Any[12, "fault status"], Any[13, "part fault id"], Any[13, "skill id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "company id"], Any[0, "company type"], Any[0, "company name"], Any[0, "company address"], Any[0, "other company details"], Any[1, "maintenance contract id"], Any[1, "maintenance contract company id"], Any[1, "contract start date"], Any[1, "contract end date"], Any[1, "other contract details"], Any[2, "part id"], Any[2, "part name"], Any[2, "chargeable yn"], Any[2, "chargeable amount"], Any[2, "other part details"], Any[3, "skill id"], Any[3, "skill code"], Any[3, "skill description"], Any[4, "staff id"], Any[4, "staff name"], Any[4, "gender"], Any[4, "other staff details"], Any[5, "asset id"], Any[5, "maintenance contract id"], Any[5, "supplier company id"], Any[5, "asset details"], Any[5, "asset make"], Any[5, "asset model"], Any[5, "asset acquired date"], Any[5, "asset disposed date"], Any[5, "other asset details"], Any[6, "asset id"], Any[6, "part id"], Any[7, "engineer id"], Any[7, "company id"], Any[7, "first name"], Any[7, "last name"], Any[7, "other details"], Any[8, "engineer id"], Any[8, "skill id"], Any[9, "fault log entry id"], Any[9, "asset id"], Any[9, "recorded by staff id"], Any[9, "fault log entry datetime"], Any[9, "fault description"], Any[9, "other fault details"], Any[10, "engineer visit id"], Any[10, "contact staff id"], Any[10, "engineer id"], Any[10, "fault log entry id"], Any[10, "fault status"], Any[10, "visit start datetime"], Any[10, "visit end datetime"], Any[10, "other visit details"], Any[11, "part fault id"], Any[11, "part id"], Any[11, "fault short name"], Any[11, "fault description"], Any[11, "other fault details"], Any[12, "fault log entry id"], Any[12, "part fault id"], Any[12, "fault status"], Any[13, "part fault id"], Any[13, "skill id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "company id"], Any[0, "company type"], Any[0, "company name"], Any[0, "company address"], Any[0, "other company details"], Any[1, "maintenance contract id"], Any[1, "maintenance contract company id"], Any[1, "contract start date"], Any[1, "contract end date"], Any[1, "other contract details"], Any[2, "part id"], Any[2, "part name"], Any[2, "chargeable yn"], Any[2, "chargeable amount"], Any[2, "other part details"], Any[3, "skill id"], Any[3, "skill code"], Any[3, "skill description"], Any[4, "staff id"], Any[4, "staff name"], Any[4, "gender"], Any[4, "other staff details"], Any[5, "asset id"], Any[5, "maintenance contract id"], Any[5, "supplier company id"], Any[5, "asset details"], Any[5, "asset make"], Any[5, "asset model"], Any[5, "asset acquired date"], Any[5, "asset disposed date"], Any[5, "other asset details"], Any[6, "asset id"], Any[6, "part id"], Any[7, "engineer id"], Any[7, "company id"], Any[7, "first name"], Any[7, "last name"], Any[7, "other details"], Any[8, "engineer id"], Any[8, "skill id"], Any[9, "fault log entry id"], Any[9, "asset id"], Any[9, "recorded by staff id"], Any[9, "fault log entry datetime"], Any[9, "fault description"], Any[9, "other fault details"], Any[10, "engineer visit id"], Any[10, "contact staff id"], Any[10, "engineer id"], Any[10, "fault log entry id"], Any[10, "fault status"], Any[10, "visit start datetime"], Any[10, "visit end datetime"], Any[10, "other visit details"], Any[11, "part fault id"], Any[11, "part id"], Any[11, "fault short name"], Any[11, "fault description"], Any[11, "other fault details"], Any[12, "fault log entry id"], Any[12, "part fault id"], Any[12, "fault status"], Any[13, "part fault id"], Any[13, "skill id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "company id"], Any[0, "company type"], Any[0, "company name"], Any[0, "company address"], Any[0, "other company details"], Any[1, "maintenance contract id"], Any[1, "maintenance contract company id"], Any[1, "contract start date"], Any[1, "contract end date"], Any[1, "other contract details"], Any[2, "part id"], Any[2, "part name"], Any[2, "chargeable yn"], Any[2, "chargeable amount"], Any[2, "other part details"], Any[3, "skill id"], Any[3, "skill code"], Any[3, "skill description"], Any[4, "staff id"], Any[4, "staff name"], Any[4, "gender"], Any[4, "other staff details"], Any[5, "asset id"], Any[5, "maintenance contract id"], Any[5, "supplier company id"], Any[5, "asset details"], Any[5, "asset make"], Any[5, "asset model"], Any[5, "asset acquired date"], Any[5, "asset disposed date"], Any[5, "other asset details"], Any[6, "asset id"], Any[6, "part id"], Any[7, "engineer id"], Any[7, "company id"], Any[7, "first name"], Any[7, "last name"], Any[7, "other details"], Any[8, "engineer id"], Any[8, "skill id"], Any[9, "fault log entry id"], Any[9, "asset id"], Any[9, "recorded by staff id"], Any[9, "fault log entry datetime"], Any[9, "fault description"], Any[9, "other fault details"], Any[10, "engineer visit id"], Any[10, "contact staff id"], Any[10, "engineer id"], Any[10, "fault log entry id"], Any[10, "fault status"], Any[10, "visit start datetime"], Any[10, "visit end datetime"], Any[10, "other visit details"], Any[11, "part fault id"], Any[11, "part id"], Any[11, "fault short name"], Any[11, "fault description"], Any[11, "other fault details"], Any[12, "fault log entry id"], Any[12, "part fault id"], Any[12, "fault status"], Any[13, "part fault id"], Any[13, "skill id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "company id"], Any[0, "company type"], Any[0, "company name"], Any[0, "company address"], Any[0, "other company details"], Any[1, "maintenance contract id"], Any[1, "maintenance contract company id"], Any[1, "contract start date"], Any[1, "contract end date"], Any[1, "other contract details"], Any[2, "part id"], Any[2, "part name"], Any[2, "chargeable yn"], Any[2, "chargeable amount"], Any[2, "other part details"], Any[3, "skill id"], Any[3, "skill code"], Any[3, "skill description"], Any[4, "staff id"], Any[4, "staff name"], Any[4, "gender"], Any[4, "other staff details"], Any[5, "asset id"], Any[5, "maintenance contract id"], Any[5, "supplier company id"], Any[5, "asset details"], Any[5, "asset make"], Any[5, "asset model"], Any[5, "asset acquired date"], Any[5, "asset disposed date"], Any[5, "other asset details"], Any[6, "asset id"], Any[6, "part id"], Any[7, "engineer id"], Any[7, "company id"], Any[7, "first name"], Any[7, "last name"], Any[7, "other details"], Any[8, "engineer id"], Any[8, "skill id"], Any[9, "fault log entry id"], Any[9, "asset id"], Any[9, "recorded by staff id"], Any[9, "fault log entry datetime"], Any[9, "fault description"], Any[9, "other fault details"], Any[10, "engineer visit id"], Any[10, "contact staff id"], Any[10, "engineer id"], Any[10, "fault log entry id"], Any[10, "fault status"], Any[10, "visit start datetime"], Any[10, "visit end datetime"], Any[10, "other visit details"], Any[11, "part fault id"], Any[11, "part id"], Any[11, "fault short name"], Any[11, "fault description"], Any[11, "other fault details"], Any[12, "fault log entry id"], Any[12, "part fault id"], Any[12, "fault status"], Any[13, "part fault id"], Any[13, "skill id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[25, 1], Any[24, 6], Any[32, 23], Any[33, 11], Any[35, 1], Any[40, 16], Any[39, 34], Any[43, 19], Any[42, 23], Any[48, 19], Any[49, 34], Any[50, 41], Any[56, 11], Any[60, 41], Any[61, 55], Any[64, 16], Any[63, 55]])
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







PClean.@model AssetsMaintenanceModel begin
    @class Third_party_companies begin
        company_type ~ ChooseUniformly(possibilities[:company_type])
        company_name ~ ChooseUniformly(possibilities[:company_name])
        company_address ~ ChooseUniformly(possibilities[:company_address])
        other_company_details ~ ChooseUniformly(possibilities[:other_company_details])
    end

    @class Maintenance_contracts begin
        third_party_companies ~ Third_party_companies
        contract_start_date ~ TimePrior(possibilities[:contract_start_date])
        contract_end_date ~ TimePrior(possibilities[:contract_end_date])
        other_contract_details ~ ChooseUniformly(possibilities[:other_contract_details])
    end

    @class Parts begin
        part_name ~ ChooseUniformly(possibilities[:part_name])
        chargeable_yn ~ ChooseUniformly(possibilities[:chargeable_yn])
        chargeable_amount ~ ChooseUniformly(possibilities[:chargeable_amount])
        other_part_details ~ ChooseUniformly(possibilities[:other_part_details])
    end

    @class Skills begin
        skill_code ~ ChooseUniformly(possibilities[:skill_code])
        skill_description ~ ChooseUniformly(possibilities[:skill_description])
    end

    @class Staff begin
        staff_name ~ ChooseUniformly(possibilities[:staff_name])
        gender ~ ChooseUniformly(possibilities[:gender])
        other_staff_details ~ ChooseUniformly(possibilities[:other_staff_details])
    end

    @class Assets begin
        maintenance_contracts ~ Maintenance_contracts
        third_party_companies ~ Third_party_companies
        asset_details ~ ChooseUniformly(possibilities[:asset_details])
        asset_make ~ ChooseUniformly(possibilities[:asset_make])
        asset_model ~ ChooseUniformly(possibilities[:asset_model])
        asset_acquired_date ~ TimePrior(possibilities[:asset_acquired_date])
        asset_disposed_date ~ TimePrior(possibilities[:asset_disposed_date])
        other_asset_details ~ ChooseUniformly(possibilities[:other_asset_details])
    end

    @class Asset_parts begin
        assets ~ Assets
        parts ~ Parts
    end

    @class Maintenance_engineers begin
        third_party_companies ~ Third_party_companies
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        other_details ~ ChooseUniformly(possibilities[:other_details])
    end

    @class Engineer_skills begin
        maintenance_engineers ~ Maintenance_engineers
        skills ~ Skills
    end

    @class Fault_log begin
        assets ~ Assets
        staff ~ Staff
        fault_log_entry_datetime ~ TimePrior(possibilities[:fault_log_entry_datetime])
        fault_description ~ ChooseUniformly(possibilities[:fault_description])
        other_fault_details ~ ChooseUniformly(possibilities[:other_fault_details])
    end

    @class Engineer_visits begin
        staff ~ Staff
        maintenance_engineers ~ Maintenance_engineers
        fault_log ~ Fault_log
        fault_status ~ ChooseUniformly(possibilities[:fault_status])
        visit_start_datetime ~ TimePrior(possibilities[:visit_start_datetime])
        visit_end_datetime ~ TimePrior(possibilities[:visit_end_datetime])
        other_visit_details ~ ChooseUniformly(possibilities[:other_visit_details])
    end

    @class Part_faults begin
        parts ~ Parts
        fault_short_name ~ ChooseUniformly(possibilities[:fault_short_name])
        fault_description ~ ChooseUniformly(possibilities[:fault_description])
        other_fault_details ~ ChooseUniformly(possibilities[:other_fault_details])
    end

    @class Fault_log_parts begin
        fault_log ~ Fault_log
        part_faults ~ Part_faults
        fault_status ~ ChooseUniformly(possibilities[:fault_status])
    end

    @class Skills_required_to_fix begin
        part_faults ~ Part_faults
        skills ~ Skills
    end

    @class Obs begin
        asset_parts ~ Asset_parts
        engineer_skills ~ Engineer_skills
        engineer_visits ~ Engineer_visits
        fault_log_parts ~ Fault_log_parts
        skills_required_to_fix ~ Skills_required_to_fix
    end
end

query = @query AssetsMaintenanceModel.Obs [
    third_party_companies_company_id asset_parts.assets.maintenance_contracts.third_party_companies.company_id
    third_party_companies_company_type asset_parts.assets.maintenance_contracts.third_party_companies.company_type
    third_party_companies_company_name asset_parts.assets.maintenance_contracts.third_party_companies.company_name
    third_party_companies_company_address asset_parts.assets.maintenance_contracts.third_party_companies.company_address
    third_party_companies_other_company_details asset_parts.assets.maintenance_contracts.third_party_companies.other_company_details
    maintenance_contracts_maintenance_contract_id asset_parts.assets.maintenance_contracts.maintenance_contract_id
    maintenance_contracts_contract_start_date asset_parts.assets.maintenance_contracts.contract_start_date
    maintenance_contracts_contract_end_date asset_parts.assets.maintenance_contracts.contract_end_date
    maintenance_contracts_other_contract_details asset_parts.assets.maintenance_contracts.other_contract_details
    parts_part_id asset_parts.parts.part_id
    parts_part_name asset_parts.parts.part_name
    parts_chargeable_yn asset_parts.parts.chargeable_yn
    parts_chargeable_amount asset_parts.parts.chargeable_amount
    parts_other_part_details asset_parts.parts.other_part_details
    skills_skill_id engineer_skills.skills.skill_id
    skills_skill_code engineer_skills.skills.skill_code
    skills_skill_description engineer_skills.skills.skill_description
    staff_id engineer_visits.fault_log.staff.staff_id
    staff_name engineer_visits.fault_log.staff.staff_name
    staff_gender engineer_visits.fault_log.staff.gender
    other_staff_details engineer_visits.fault_log.staff.other_staff_details
    assets_asset_id asset_parts.assets.asset_id
    assets_asset_details asset_parts.assets.asset_details
    assets_asset_make asset_parts.assets.asset_make
    assets_asset_model asset_parts.assets.asset_model
    assets_asset_acquired_date asset_parts.assets.asset_acquired_date
    assets_asset_disposed_date asset_parts.assets.asset_disposed_date
    assets_other_asset_details asset_parts.assets.other_asset_details
    maintenance_engineers_engineer_id engineer_skills.maintenance_engineers.engineer_id
    maintenance_engineers_first_name engineer_skills.maintenance_engineers.first_name
    maintenance_engineers_last_name engineer_skills.maintenance_engineers.last_name
    maintenance_engineers_other_details engineer_skills.maintenance_engineers.other_details
    fault_log_entry_id engineer_visits.fault_log.fault_log_entry_id
    fault_log_entry_datetime engineer_visits.fault_log.fault_log_entry_datetime
    fault_log_fault_description engineer_visits.fault_log.fault_description
    fault_log_other_fault_details engineer_visits.fault_log.other_fault_details
    engineer_visits_engineer_visit_id engineer_visits.engineer_visit_id
    engineer_visits_fault_status engineer_visits.fault_status
    engineer_visits_visit_start_datetime engineer_visits.visit_start_datetime
    engineer_visits_visit_end_datetime engineer_visits.visit_end_datetime
    engineer_visits_other_visit_details engineer_visits.other_visit_details
    part_faults_part_fault_id fault_log_parts.part_faults.part_fault_id
    part_faults_fault_short_name fault_log_parts.part_faults.fault_short_name
    part_faults_fault_description fault_log_parts.part_faults.fault_description
    part_faults_other_fault_details fault_log_parts.part_faults.other_fault_details
    fault_log_parts_fault_status fault_log_parts.fault_status
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
