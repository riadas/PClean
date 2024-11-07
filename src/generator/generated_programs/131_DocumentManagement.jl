using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("roles_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("roles_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "role code"], Any[0, "role description"], Any[1, "user id"], Any[1, "role code"], Any[1, "user name"], Any[1, "user login"], Any[1, "password"], Any[2, "document structure code"], Any[2, "parent document structure code"], Any[2, "document structure description"], Any[3, "functional area code"], Any[3, "parent functional area code"], Any[3, "functional area description"], Any[4, "image id"], Any[4, "image alt text"], Any[4, "image name"], Any[4, "image url"], Any[5, "document code"], Any[5, "document structure code"], Any[5, "document type code"], Any[5, "access count"], Any[5, "document name"], Any[6, "document code"], Any[6, "functional area code"], Any[7, "section id"], Any[7, "document code"], Any[7, "section sequence"], Any[7, "section code"], Any[7, "section title"], Any[8, "section id"], Any[8, "image id"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "role code"], Any[0, "role description"], Any[1, "user id"], Any[1, "role code"], Any[1, "user name"], Any[1, "user login"], Any[1, "password"], Any[2, "document structure code"], Any[2, "parent document structure code"], Any[2, "document structure description"], Any[3, "functional area code"], Any[3, "parent functional area code"], Any[3, "functional area description"], Any[4, "image id"], Any[4, "image alt text"], Any[4, "image name"], Any[4, "image url"], Any[5, "document code"], Any[5, "document structure code"], Any[5, "document type code"], Any[5, "access count"], Any[5, "document name"], Any[6, "document code"], Any[6, "functional area code"], Any[7, "section id"], Any[7, "document code"], Any[7, "section sequence"], Any[7, "section code"], Any[7, "section title"], Any[8, "section id"], Any[8, "image id"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model DocumentManagementModel begin
    @class Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class Users begin
        user_id ~ Unmodeled()
        role_code ~ ChooseUniformly(possibilities[:role_code])
        user_name ~ ChooseUniformly(possibilities[:user_name])
        user_login ~ ChooseUniformly(possibilities[:user_login])
        password ~ ChooseUniformly(possibilities[:password])
    end

    @class Document_Structures begin
        document_structure_code ~ ChooseUniformly(possibilities[:document_structure_code])
        parent_document_structure_code ~ ChooseUniformly(possibilities[:parent_document_structure_code])
        document_structure_description ~ ChooseUniformly(possibilities[:document_structure_description])
    end

    @class Functional_Areas begin
        functional_area_code ~ ChooseUniformly(possibilities[:functional_area_code])
        parent_functional_area_code ~ ChooseUniformly(possibilities[:parent_functional_area_code])
        functional_area_description ~ ChooseUniformly(possibilities[:functional_area_description])
    end

    @class Images begin
        image_id ~ Unmodeled()
        image_alt_text ~ ChooseUniformly(possibilities[:image_alt_text])
        image_name ~ ChooseUniformly(possibilities[:image_name])
        image_url ~ ChooseUniformly(possibilities[:image_url])
    end

    @class Documents begin
        document_code ~ ChooseUniformly(possibilities[:document_code])
        document_structure_code ~ ChooseUniformly(possibilities[:document_structure_code])
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        access_count ~ ChooseUniformly(possibilities[:access_count])
        document_name ~ ChooseUniformly(possibilities[:document_name])
    end

    @class Document_Functional_Areas begin
        document_code ~ ChooseUniformly(possibilities[:document_code])
        functional_area_code ~ ChooseUniformly(possibilities[:functional_area_code])
    end

    @class Document_Sections begin
        section_id ~ Unmodeled()
        document_code ~ ChooseUniformly(possibilities[:document_code])
        section_sequence ~ ChooseUniformly(possibilities[:section_sequence])
        section_code ~ ChooseUniformly(possibilities[:section_code])
        section_title ~ ChooseUniformly(possibilities[:section_title])
    end

    @class Document_Sections_Images begin
        section_id ~ Unmodeled()
        image_id ~ ChooseUniformly(possibilities[:image_id])
    end

    @class Obs begin
        roles ~ Roles
        users ~ Users
        document_Structures ~ Document_Structures
        functional_Areas ~ Functional_Areas
        images ~ Images
        documents ~ Documents
        document_Functional_Areas ~ Document_Functional_Areas
        document_Sections ~ Document_Sections
        document_Sections_Images ~ Document_Sections_Images
    end
end

query = @query DocumentManagementModel.Obs [
    roles_role_code roles.role_code
    roles_role_description roles.role_description
    users_user_id users.user_id
    users_user_name users.user_name
    users_user_login users.user_login
    users_password users.password
    document_structures_document_structure_code document_Structures.document_structure_code
    document_structures_parent_document_structure_code document_Structures.parent_document_structure_code
    document_structures_document_structure_description document_Structures.document_structure_description
    functional_areas_functional_area_code functional_Areas.functional_area_code
    functional_areas_parent_functional_area_code functional_Areas.parent_functional_area_code
    functional_areas_functional_area_description functional_Areas.functional_area_description
    images_image_id images.image_id
    images_image_alt_text images.image_alt_text
    images_image_name images.image_name
    images_image_url images.image_url
    documents_document_code documents.document_code
    documents_document_type_code documents.document_type_code
    documents_access_count documents.access_count
    documents_document_name documents.document_name
    document_sections_section_id document_Sections.section_id
    document_sections_section_sequence document_Sections.section_sequence
    document_sections_section_code document_Sections.section_code
    document_sections_section_title document_Sections.section_title
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
