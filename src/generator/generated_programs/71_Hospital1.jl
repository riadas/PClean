using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("physician_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("physician_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "position"], Any[0, "ssn"], Any[1, "departmentid"], Any[1, "name"], Any[1, "head"], Any[2, "physician"], Any[2, "department"], Any[2, "primary affiliation"], Any[3, "code"], Any[3, "name"], Any[3, "cost"], Any[4, "physician"], Any[4, "treatment"], Any[4, "certification date"], Any[4, "certification expires"], Any[5, "ssn"], Any[5, "name"], Any[5, "address"], Any[5, "phone"], Any[5, "insurance id"], Any[5, "pcp"], Any[6, "employee id"], Any[6, "name"], Any[6, "position"], Any[6, "registered"], Any[6, "ssn"], Any[7, "appointment id"], Any[7, "patient"], Any[7, "prep nurse"], Any[7, "physician"], Any[7, "start"], Any[7, "end"], Any[7, "examination room"], Any[8, "code"], Any[8, "name"], Any[8, "brand"], Any[8, "description"], Any[9, "physician"], Any[9, "patient"], Any[9, "medication"], Any[9, "date"], Any[9, "appointment"], Any[9, "dose"], Any[10, "block floor"], Any[10, "block code"], Any[11, "roomnumber"], Any[11, "room type"], Any[11, "block floor"], Any[11, "block code"], Any[11, "unavailable"], Any[12, "nurse"], Any[12, "block floor"], Any[12, "block code"], Any[12, "oncall start"], Any[12, "oncall end"], Any[13, "stay id"], Any[13, "patient"], Any[13, "room"], Any[13, "stay start"], Any[13, "stay end"], Any[14, "patient"], Any[14, "procedures"], Any[14, "stay"], Any[14, "date undergoes"], Any[14, "physician"], Any[14, "assisting nurse"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "employee id"], Any[0, "name"], Any[0, "position"], Any[0, "ssn"], Any[1, "departmentid"], Any[1, "name"], Any[1, "head"], Any[2, "physician"], Any[2, "department"], Any[2, "primary affiliation"], Any[3, "code"], Any[3, "name"], Any[3, "cost"], Any[4, "physician"], Any[4, "treatment"], Any[4, "certification date"], Any[4, "certification expires"], Any[5, "ssn"], Any[5, "name"], Any[5, "address"], Any[5, "phone"], Any[5, "insurance id"], Any[5, "pcp"], Any[6, "employee id"], Any[6, "name"], Any[6, "position"], Any[6, "registered"], Any[6, "ssn"], Any[7, "appointment id"], Any[7, "patient"], Any[7, "prep nurse"], Any[7, "physician"], Any[7, "start"], Any[7, "end"], Any[7, "examination room"], Any[8, "code"], Any[8, "name"], Any[8, "brand"], Any[8, "description"], Any[9, "physician"], Any[9, "patient"], Any[9, "medication"], Any[9, "date"], Any[9, "appointment"], Any[9, "dose"], Any[10, "block floor"], Any[10, "block code"], Any[11, "roomnumber"], Any[11, "room type"], Any[11, "block floor"], Any[11, "block code"], Any[11, "unavailable"], Any[12, "nurse"], Any[12, "block floor"], Any[12, "block code"], Any[12, "oncall start"], Any[12, "oncall end"], Any[13, "stay id"], Any[13, "patient"], Any[13, "room"], Any[13, "stay start"], Any[13, "stay end"], Any[14, "patient"], Any[14, "procedures"], Any[14, "stay"], Any[14, "date undergoes"], Any[14, "physician"], Any[14, "assisting nurse"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model Hospital1Model begin
    @class Physician begin
        employee_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        position ~ ChooseUniformly(possibilities[:position])
        ssn ~ ChooseUniformly(possibilities[:ssn])
    end

    @class Department begin
        departmentid ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        head ~ ChooseUniformly(possibilities[:head])
    end

    @class Affiliated_With begin
        physician ~ ChooseUniformly(possibilities[:physician])
        department ~ ChooseUniformly(possibilities[:department])
        primary_affiliation ~ ChooseUniformly(possibilities[:primary_affiliation])
    end

    @class Procedures begin
        code ~ ChooseUniformly(possibilities[:code])
        name ~ ChooseUniformly(possibilities[:name])
        cost ~ ChooseUniformly(possibilities[:cost])
    end

    @class Trained_In begin
        physician ~ ChooseUniformly(possibilities[:physician])
        treatment ~ ChooseUniformly(possibilities[:treatment])
        certification_date ~ TimePrior(possibilities[:certification_date])
        certification_expires ~ TimePrior(possibilities[:certification_expires])
    end

    @class Patient begin
        ssn ~ ChooseUniformly(possibilities[:ssn])
        name ~ ChooseUniformly(possibilities[:name])
        address ~ ChooseUniformly(possibilities[:address])
        phone ~ ChooseUniformly(possibilities[:phone])
        insurance_id ~ ChooseUniformly(possibilities[:insurance_id])
        pcp ~ ChooseUniformly(possibilities[:pcp])
    end

    @class Nurse begin
        employee_id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        position ~ ChooseUniformly(possibilities[:position])
        registered ~ ChooseUniformly(possibilities[:registered])
        ssn ~ ChooseUniformly(possibilities[:ssn])
    end

    @class Appointment begin
        appointment_id ~ Unmodeled()
        patient ~ ChooseUniformly(possibilities[:patient])
        prep_nurse ~ ChooseUniformly(possibilities[:prep_nurse])
        physician ~ ChooseUniformly(possibilities[:physician])
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
        physician ~ ChooseUniformly(possibilities[:physician])
        patient ~ ChooseUniformly(possibilities[:patient])
        medication ~ ChooseUniformly(possibilities[:medication])
        date ~ TimePrior(possibilities[:date])
        appointment ~ ChooseUniformly(possibilities[:appointment])
        dose ~ ChooseUniformly(possibilities[:dose])
    end

    @class Block begin
        block_floor ~ ChooseUniformly(possibilities[:block_floor])
        block_code ~ ChooseUniformly(possibilities[:block_code])
    end

    @class Room begin
        roomnumber ~ ChooseUniformly(possibilities[:roomnumber])
        room_type ~ ChooseUniformly(possibilities[:room_type])
        block_floor ~ ChooseUniformly(possibilities[:block_floor])
        block_code ~ ChooseUniformly(possibilities[:block_code])
        unavailable ~ ChooseUniformly(possibilities[:unavailable])
    end

    @class On_Call begin
        nurse ~ ChooseUniformly(possibilities[:nurse])
        block_floor ~ ChooseUniformly(possibilities[:block_floor])
        block_code ~ ChooseUniformly(possibilities[:block_code])
        oncall_start ~ TimePrior(possibilities[:oncall_start])
        oncall_end ~ TimePrior(possibilities[:oncall_end])
    end

    @class Stay begin
        stay_id ~ Unmodeled()
        patient ~ ChooseUniformly(possibilities[:patient])
        room ~ ChooseUniformly(possibilities[:room])
        stay_start ~ TimePrior(possibilities[:stay_start])
        stay_end ~ TimePrior(possibilities[:stay_end])
    end

    @class Undergoes begin
        patient ~ ChooseUniformly(possibilities[:patient])
        procedures ~ ChooseUniformly(possibilities[:procedures])
        stay ~ ChooseUniformly(possibilities[:stay])
        date_undergoes ~ TimePrior(possibilities[:date_undergoes])
        physician ~ ChooseUniformly(possibilities[:physician])
        assisting_nurse ~ ChooseUniformly(possibilities[:assisting_nurse])
    end

    @class Obs begin
        physician ~ Physician
        department ~ Department
        affiliated_With ~ Affiliated_With
        procedures ~ Procedures
        trained_In ~ Trained_In
        patient ~ Patient
        nurse ~ Nurse
        appointment ~ Appointment
        medication ~ Medication
        prescribes ~ Prescribes
        block ~ Block
        room ~ Room
        on_Call ~ On_Call
        stay ~ Stay
        undergoes ~ Undergoes
    end
end

query = @query Hospital1Model.Obs [
    physician_employee_id physician.employee_id
    physician_name physician.name
    physician_position physician.position
    physician_ssn physician.ssn
    departmentid department.departmentid
    department_name department.name
    affiliated_with_primary_affiliation affiliated_With.primary_affiliation
    procedures_code procedures.code
    procedures_name procedures.name
    procedures_cost procedures.cost
    trained_in_certification_date trained_In.certification_date
    trained_in_certification_expires trained_In.certification_expires
    patient_ssn patient.ssn
    patient_name patient.name
    patient_address patient.address
    patient_phone patient.phone
    patient_insurance_id patient.insurance_id
    nurse_employee_id nurse.employee_id
    nurse_name nurse.name
    nurse_position nurse.position
    nurse_registered nurse.registered
    nurse_ssn nurse.ssn
    appointment_id appointment.appointment_id
    appointment_start appointment.start
    appointment_end appointment.end
    appointment_examination_room appointment.examination_room
    medication_code medication.code
    medication_name medication.name
    medication_brand medication.brand
    medication_description medication.description
    prescribes_date prescribes.date
    prescribes_dose prescribes.dose
    block_floor block.block_floor
    block_code block.block_code
    roomnumber room.roomnumber
    room_type room.room_type
    room_unavailable room.unavailable
    on_call_oncall_start on_Call.oncall_start
    on_call_oncall_end on_Call.oncall_end
    stay_id stay.stay_id
    stay_start stay.stay_start
    stay_end stay.stay_end
    date_undergoes undergoes.date_undergoes
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
