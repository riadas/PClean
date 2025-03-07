using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("physician_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("physician_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "position"], Any[0, "ssn"], Any[1, "departmentid"], Any[1, "name"], Any[1, "head"], Any[2, "physician"], Any[2, "department"], Any[2, "primary affiliation"], Any[3, "code"], Any[3, "name"], Any[3, "cost"], Any[4, "physician"], Any[4, "treatment"], Any[4, "certification date"], Any[4, "certification expires"], Any[5, "ssn"], Any[5, "name"], Any[5, "address"], Any[5, "phone"], Any[5, "insurance id"], Any[5, "pcp"], Any[6, "employee id"], Any[6, "name"], Any[6, "position"], Any[6, "registered"], Any[6, "ssn"], Any[7, "appointment id"], Any[7, "patient"], Any[7, "prep nurse"], Any[7, "physician"], Any[7, "start"], Any[7, "end"], Any[7, "examination room"], Any[8, "code"], Any[8, "name"], Any[8, "brand"], Any[8, "description"], Any[9, "physician"], Any[9, "patient"], Any[9, "medication"], Any[9, "date"], Any[9, "appointment"], Any[9, "dose"], Any[10, "block floor"], Any[10, "block code"], Any[11, "roomnumber"], Any[11, "room type"], Any[11, "block floor"], Any[11, "block code"], Any[11, "unavailable"], Any[12, "nurse"], Any[12, "block floor"], Any[12, "block code"], Any[12, "oncall start"], Any[12, "oncall end"], Any[13, "stay id"], Any[13, "patient"], Any[13, "room"], Any[13, "stay start"], Any[13, "stay end"], Any[14, "patient"], Any[14, "procedures"], Any[14, "stay"], Any[14, "date undergoes"], Any[14, "physician"], Any[14, "assisting nurse"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "position"], Any[0, "ssn"], Any[1, "departmentid"], Any[1, "name"], Any[1, "head"], Any[2, "physician"], Any[2, "department"], Any[2, "primary affiliation"], Any[3, "code"], Any[3, "name"], Any[3, "cost"], Any[4, "physician"], Any[4, "treatment"], Any[4, "certification date"], Any[4, "certification expires"], Any[5, "ssn"], Any[5, "name"], Any[5, "address"], Any[5, "phone"], Any[5, "insurance id"], Any[5, "pcp"], Any[6, "employee id"], Any[6, "name"], Any[6, "position"], Any[6, "registered"], Any[6, "ssn"], Any[7, "appointment id"], Any[7, "patient"], Any[7, "prep nurse"], Any[7, "physician"], Any[7, "start"], Any[7, "end"], Any[7, "examination room"], Any[8, "code"], Any[8, "name"], Any[8, "brand"], Any[8, "description"], Any[9, "physician"], Any[9, "patient"], Any[9, "medication"], Any[9, "date"], Any[9, "appointment"], Any[9, "dose"], Any[10, "block floor"], Any[10, "block code"], Any[11, "roomnumber"], Any[11, "room type"], Any[11, "block floor"], Any[11, "block code"], Any[11, "unavailable"], Any[12, "nurse"], Any[12, "block floor"], Any[12, "block code"], Any[12, "oncall start"], Any[12, "oncall end"], Any[13, "stay id"], Any[13, "patient"], Any[13, "room"], Any[13, "stay start"], Any[13, "stay end"], Any[14, "patient"], Any[14, "procedures"], Any[14, "stay"], Any[14, "date undergoes"], Any[14, "physician"], Any[14, "assisting nurse"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "position"], Any[0, "ssn"], Any[1, "departmentid"], Any[1, "name"], Any[1, "head"], Any[2, "physician"], Any[2, "department"], Any[2, "primary affiliation"], Any[3, "code"], Any[3, "name"], Any[3, "cost"], Any[4, "physician"], Any[4, "treatment"], Any[4, "certification date"], Any[4, "certification expires"], Any[5, "ssn"], Any[5, "name"], Any[5, "address"], Any[5, "phone"], Any[5, "insurance id"], Any[5, "pcp"], Any[6, "employee id"], Any[6, "name"], Any[6, "position"], Any[6, "registered"], Any[6, "ssn"], Any[7, "appointment id"], Any[7, "patient"], Any[7, "prep nurse"], Any[7, "physician"], Any[7, "start"], Any[7, "end"], Any[7, "examination room"], Any[8, "code"], Any[8, "name"], Any[8, "brand"], Any[8, "description"], Any[9, "physician"], Any[9, "patient"], Any[9, "medication"], Any[9, "date"], Any[9, "appointment"], Any[9, "dose"], Any[10, "block floor"], Any[10, "block code"], Any[11, "roomnumber"], Any[11, "room type"], Any[11, "block floor"], Any[11, "block code"], Any[11, "unavailable"], Any[12, "nurse"], Any[12, "block floor"], Any[12, "block code"], Any[12, "oncall start"], Any[12, "oncall end"], Any[13, "stay id"], Any[13, "patient"], Any[13, "room"], Any[13, "stay start"], Any[13, "stay end"], Any[14, "patient"], Any[14, "procedures"], Any[14, "stay"], Any[14, "date undergoes"], Any[14, "physician"], Any[14, "assisting nurse"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "position"], Any[0, "ssn"], Any[1, "departmentid"], Any[1, "name"], Any[1, "head"], Any[2, "physician"], Any[2, "department"], Any[2, "primary affiliation"], Any[3, "code"], Any[3, "name"], Any[3, "cost"], Any[4, "physician"], Any[4, "treatment"], Any[4, "certification date"], Any[4, "certification expires"], Any[5, "ssn"], Any[5, "name"], Any[5, "address"], Any[5, "phone"], Any[5, "insurance id"], Any[5, "pcp"], Any[6, "employee id"], Any[6, "name"], Any[6, "position"], Any[6, "registered"], Any[6, "ssn"], Any[7, "appointment id"], Any[7, "patient"], Any[7, "prep nurse"], Any[7, "physician"], Any[7, "start"], Any[7, "end"], Any[7, "examination room"], Any[8, "code"], Any[8, "name"], Any[8, "brand"], Any[8, "description"], Any[9, "physician"], Any[9, "patient"], Any[9, "medication"], Any[9, "date"], Any[9, "appointment"], Any[9, "dose"], Any[10, "block floor"], Any[10, "block code"], Any[11, "roomnumber"], Any[11, "room type"], Any[11, "block floor"], Any[11, "block code"], Any[11, "unavailable"], Any[12, "nurse"], Any[12, "block floor"], Any[12, "block code"], Any[12, "oncall start"], Any[12, "oncall end"], Any[13, "stay id"], Any[13, "patient"], Any[13, "room"], Any[13, "stay start"], Any[13, "stay end"], Any[14, "patient"], Any[14, "procedures"], Any[14, "stay"], Any[14, "date undergoes"], Any[14, "physician"], Any[14, "assisting nurse"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "position"], Any[0, "ssn"], Any[1, "departmentid"], Any[1, "name"], Any[1, "head"], Any[2, "physician"], Any[2, "department"], Any[2, "primary affiliation"], Any[3, "code"], Any[3, "name"], Any[3, "cost"], Any[4, "physician"], Any[4, "treatment"], Any[4, "certification date"], Any[4, "certification expires"], Any[5, "ssn"], Any[5, "name"], Any[5, "address"], Any[5, "phone"], Any[5, "insurance id"], Any[5, "pcp"], Any[6, "employee id"], Any[6, "name"], Any[6, "position"], Any[6, "registered"], Any[6, "ssn"], Any[7, "appointment id"], Any[7, "patient"], Any[7, "prep nurse"], Any[7, "physician"], Any[7, "start"], Any[7, "end"], Any[7, "examination room"], Any[8, "code"], Any[8, "name"], Any[8, "brand"], Any[8, "description"], Any[9, "physician"], Any[9, "patient"], Any[9, "medication"], Any[9, "date"], Any[9, "appointment"], Any[9, "dose"], Any[10, "block floor"], Any[10, "block code"], Any[11, "roomnumber"], Any[11, "room type"], Any[11, "block floor"], Any[11, "block code"], Any[11, "unavailable"], Any[12, "nurse"], Any[12, "block floor"], Any[12, "block code"], Any[12, "oncall start"], Any[12, "oncall end"], Any[13, "stay id"], Any[13, "patient"], Any[13, "room"], Any[13, "stay start"], Any[13, "stay end"], Any[14, "patient"], Any[14, "procedures"], Any[14, "stay"], Any[14, "date undergoes"], Any[14, "physician"], Any[14, "assisting nurse"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[7, 1], Any[9, 5], Any[8, 1], Any[15, 11], Any[14, 1], Any[23, 1], Any[32, 1], Any[31, 24], Any[30, 18], Any[44, 29], Any[42, 36], Any[41, 18], Any[40, 1], Any[50, 46], Any[51, 47], Any[54, 46], Any[55, 47], Any[53, 24], Any[60, 48], Any[59, 18], Any[68, 24], Any[67, 1], Any[65, 58], Any[64, 11], Any[63, 18]])
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







PClean.@model Hospital1Model begin
    @class Physician begin
        name ~ ChooseUniformly(possibilities[:name])
        position ~ ChooseUniformly(possibilities[:position])
        ssn ~ ChooseUniformly(possibilities[:ssn])
    end

    @class Department begin
        name ~ ChooseUniformly(possibilities[:name])
        physician ~ Physician
    end

    @class Affiliated_with begin
        physician ~ Physician
        department ~ Department
        primary_affiliation ~ ChooseUniformly(possibilities[:primary_affiliation])
    end

    @class Procedures begin
        code ~ ChooseUniformly(possibilities[:code])
        name ~ ChooseUniformly(possibilities[:name])
        cost ~ ChooseUniformly(possibilities[:cost])
    end

    @class Trained_in begin
        physician ~ Physician
        procedures ~ Procedures
        certification_date ~ TimePrior(possibilities[:certification_date])
        certification_expires ~ TimePrior(possibilities[:certification_expires])
    end

    @class Patient begin
        ssn ~ ChooseUniformly(possibilities[:ssn])
        name ~ ChooseUniformly(possibilities[:name])
        address ~ ChooseUniformly(possibilities[:address])
        phone ~ ChooseUniformly(possibilities[:phone])
        insurance_id ~ ChooseUniformly(possibilities[:insurance_id])
        physician ~ Physician
    end

    @class Nurse begin
        name ~ ChooseUniformly(possibilities[:name])
        position ~ ChooseUniformly(possibilities[:position])
        registered ~ ChooseUniformly(possibilities[:registered])
        ssn ~ ChooseUniformly(possibilities[:ssn])
    end

    @class Appointment begin
        patient ~ Patient
        nurse ~ Nurse
        physician ~ Physician
        start ~ TimePrior(possibilities[:start])
        end ~ TimePrior(possibilities[:end])
        examination_room ~ ChooseUniformly(possibilities[:examination_room])
    end

    @class Medication begin
        code ~ ChooseUniformly(possibilities[:code])
        name ~ ChooseUniformly(possibilities[:name])
        brand ~ ChooseUniformly(possibilities[:brand])
        description ~ ChooseUniformly(possibilities[:description])
    end

    @class Prescribes begin
        physician ~ Physician
        patient ~ Patient
        medication ~ Medication
        date ~ TimePrior(possibilities[:date])
        appointment ~ Appointment
        dose ~ ChooseUniformly(possibilities[:dose])
    end

    @class Block begin
        block_floor ~ ChooseUniformly(possibilities[:block_floor])
        block_code ~ ChooseUniformly(possibilities[:block_code])
    end

    @class Room begin
        roomnumber ~ ChooseUniformly(possibilities[:roomnumber])
        room_type ~ ChooseUniformly(possibilities[:room_type])
        block ~ Block
        unavailable ~ ChooseUniformly(possibilities[:unavailable])
    end

    @class On_call begin
        nurse ~ Nurse
        block ~ Block
        oncall_start ~ TimePrior(possibilities[:oncall_start])
        oncall_end ~ TimePrior(possibilities[:oncall_end])
    end

    @class Stay begin
        patient ~ Patient
        room ~ Room
        stay_start ~ TimePrior(possibilities[:stay_start])
        stay_end ~ TimePrior(possibilities[:stay_end])
    end

    @class Undergoes begin
        patient ~ Patient
        procedures ~ Procedures
        stay ~ Stay
        date_undergoes ~ TimePrior(possibilities[:date_undergoes])
        physician ~ Physician
        nurse ~ Nurse
    end

    @class Obs begin
        affiliated_with ~ Affiliated_with
        trained_in ~ Trained_in
        prescribes ~ Prescribes
        on_call ~ On_call
        undergoes ~ Undergoes
    end
end

query = @query Hospital1Model.Obs [
    physician_employee_id affiliated_with.department.physician.employee_id
    physician_name affiliated_with.department.physician.name
    physician_position affiliated_with.department.physician.position
    physician_ssn affiliated_with.department.physician.ssn
    department_name affiliated_with.department.name
    affiliated_with_primary_affiliation affiliated_with.primary_affiliation
    procedures_code trained_in.procedures.code
    procedures_name trained_in.procedures.name
    procedures_cost trained_in.procedures.cost
    trained_in_certification_date trained_in.certification_date
    trained_in_certification_expires trained_in.certification_expires
    patient_ssn prescribes.appointment.patient.ssn
    patient_name prescribes.appointment.patient.name
    patient_address prescribes.appointment.patient.address
    patient_phone prescribes.appointment.patient.phone
    patient_insurance_id prescribes.appointment.patient.insurance_id
    nurse_employee_id on_call.nurse.employee_id
    nurse_name on_call.nurse.name
    nurse_position on_call.nurse.position
    nurse_registered on_call.nurse.registered
    nurse_ssn on_call.nurse.ssn
    appointment_id prescribes.appointment.appointment_id
    appointment_start prescribes.appointment.start
    appointment_end prescribes.appointment.end
    appointment_examination_room prescribes.appointment.examination_room
    medication_code prescribes.medication.code
    medication_name prescribes.medication.name
    medication_brand prescribes.medication.brand
    medication_description prescribes.medication.description
    prescribes_date prescribes.date
    prescribes_dose prescribes.dose
    block_floor on_call.block.block_floor
    block_code on_call.block.block_code
    roomnumber undergoes.stay.room.roomnumber
    room_type undergoes.stay.room.room_type
    room_unavailable undergoes.stay.room.unavailable
    on_call_oncall_start on_call.oncall_start
    on_call_oncall_end on_call.oncall_end
    stay_id undergoes.stay.stay_id
    stay_start undergoes.stay.stay_start
    stay_end undergoes.stay.stay_end
    date_undergoes undergoes.date_undergoes
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
