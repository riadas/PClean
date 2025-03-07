using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("roles_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("roles_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "role code"], Any[0, "role description"], Any[1, "user id"], Any[1, "role code"], Any[1, "user name"], Any[1, "user login"], Any[1, "password"], Any[2, "document structure code"], Any[2, "parent document structure code"], Any[2, "document structure description"], Any[3, "functional area code"], Any[3, "parent functional area code"], Any[3, "functional area description"], Any[4, "image id"], Any[4, "image alt text"], Any[4, "image name"], Any[4, "image url"], Any[5, "document code"], Any[5, "document structure code"], Any[5, "document type code"], Any[5, "access count"], Any[5, "document name"], Any[6, "document code"], Any[6, "functional area code"], Any[7, "section id"], Any[7, "document code"], Any[7, "section sequence"], Any[7, "section code"], Any[7, "section title"], Any[8, "section id"], Any[8, "image id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "role code"], Any[0, "role description"], Any[1, "user id"], Any[1, "role code"], Any[1, "user name"], Any[1, "user login"], Any[1, "password"], Any[2, "document structure code"], Any[2, "parent document structure code"], Any[2, "document structure description"], Any[3, "functional area code"], Any[3, "parent functional area code"], Any[3, "functional area description"], Any[4, "image id"], Any[4, "image alt text"], Any[4, "image name"], Any[4, "image url"], Any[5, "document code"], Any[5, "document structure code"], Any[5, "document type code"], Any[5, "access count"], Any[5, "document name"], Any[6, "document code"], Any[6, "functional area code"], Any[7, "section id"], Any[7, "document code"], Any[7, "section sequence"], Any[7, "section code"], Any[7, "section title"], Any[8, "section id"], Any[8, "image id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "role code"], Any[0, "role description"], Any[1, "user id"], Any[1, "role code"], Any[1, "user name"], Any[1, "user login"], Any[1, "password"], Any[2, "document structure code"], Any[2, "parent document structure code"], Any[2, "document structure description"], Any[3, "functional area code"], Any[3, "parent functional area code"], Any[3, "functional area description"], Any[4, "image id"], Any[4, "image alt text"], Any[4, "image name"], Any[4, "image url"], Any[5, "document code"], Any[5, "document structure code"], Any[5, "document type code"], Any[5, "access count"], Any[5, "document name"], Any[6, "document code"], Any[6, "functional area code"], Any[7, "section id"], Any[7, "document code"], Any[7, "section sequence"], Any[7, "section code"], Any[7, "section title"], Any[8, "section id"], Any[8, "image id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "role code"], Any[0, "role description"], Any[1, "user id"], Any[1, "role code"], Any[1, "user name"], Any[1, "user login"], Any[1, "password"], Any[2, "document structure code"], Any[2, "parent document structure code"], Any[2, "document structure description"], Any[3, "functional area code"], Any[3, "parent functional area code"], Any[3, "functional area description"], Any[4, "image id"], Any[4, "image alt text"], Any[4, "image name"], Any[4, "image url"], Any[5, "document code"], Any[5, "document structure code"], Any[5, "document type code"], Any[5, "access count"], Any[5, "document name"], Any[6, "document code"], Any[6, "functional area code"], Any[7, "section id"], Any[7, "document code"], Any[7, "section sequence"], Any[7, "section code"], Any[7, "section title"], Any[8, "section id"], Any[8, "image id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "role code"], Any[0, "role description"], Any[1, "user id"], Any[1, "role code"], Any[1, "user name"], Any[1, "user login"], Any[1, "password"], Any[2, "document structure code"], Any[2, "parent document structure code"], Any[2, "document structure description"], Any[3, "functional area code"], Any[3, "parent functional area code"], Any[3, "functional area description"], Any[4, "image id"], Any[4, "image alt text"], Any[4, "image name"], Any[4, "image url"], Any[5, "document code"], Any[5, "document structure code"], Any[5, "document type code"], Any[5, "access count"], Any[5, "document name"], Any[6, "document code"], Any[6, "functional area code"], Any[7, "section id"], Any[7, "document code"], Any[7, "section sequence"], Any[7, "section code"], Any[7, "section title"], Any[8, "section id"], Any[8, "image id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[4, 1], Any[19, 8], Any[24, 11], Any[23, 18], Any[26, 18], Any[31, 14], Any[30, 25]])
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







PClean.@model DocumentManagementModel begin
    @class Roles begin
        role_code ~ ChooseUniformly(possibilities[:role_code])
        role_description ~ ChooseUniformly(possibilities[:role_description])
    end

    @class Users begin
        roles ~ Roles
        user_name ~ ChooseUniformly(possibilities[:user_name])
        user_login ~ ChooseUniformly(possibilities[:user_login])
        password ~ ChooseUniformly(possibilities[:password])
    end

    @class Document_structures begin
        document_structure_code ~ ChooseUniformly(possibilities[:document_structure_code])
        parent_document_structure_code ~ ChooseUniformly(possibilities[:parent_document_structure_code])
        document_structure_description ~ ChooseUniformly(possibilities[:document_structure_description])
    end

    @class Functional_areas begin
        functional_area_code ~ ChooseUniformly(possibilities[:functional_area_code])
        parent_functional_area_code ~ ChooseUniformly(possibilities[:parent_functional_area_code])
        functional_area_description ~ ChooseUniformly(possibilities[:functional_area_description])
    end

    @class Images begin
        image_alt_text ~ ChooseUniformly(possibilities[:image_alt_text])
        image_name ~ ChooseUniformly(possibilities[:image_name])
        image_url ~ ChooseUniformly(possibilities[:image_url])
    end

    @class Documents begin
        document_code ~ ChooseUniformly(possibilities[:document_code])
        document_structures ~ Document_structures
        document_type_code ~ ChooseUniformly(possibilities[:document_type_code])
        access_count ~ ChooseUniformly(possibilities[:access_count])
        document_name ~ ChooseUniformly(possibilities[:document_name])
    end

    @class Document_functional_areas begin
        documents ~ Documents
        functional_areas ~ Functional_areas
    end

    @class Document_sections begin
        documents ~ Documents
        section_sequence ~ ChooseUniformly(possibilities[:section_sequence])
        section_code ~ ChooseUniformly(possibilities[:section_code])
        section_title ~ ChooseUniformly(possibilities[:section_title])
    end

    @class Document_sections_images begin
        images ~ Images
    end

    @class Obs begin
        users ~ Users
        document_functional_areas ~ Document_functional_areas
        document_sections_images ~ Document_sections_images
    end
end

query = @query DocumentManagementModel.Obs [
    roles_role_code users.roles.role_code
    roles_role_description users.roles.role_description
    users_user_id users.user_id
    users_user_name users.user_name
    users_user_login users.user_login
    users_password users.password
    document_structures_document_structure_code document_functional_areas.documents.document_structures.document_structure_code
    document_structures_parent_document_structure_code document_functional_areas.documents.document_structures.parent_document_structure_code
    document_structures_document_structure_description document_functional_areas.documents.document_structures.document_structure_description
    functional_areas_functional_area_code document_functional_areas.functional_areas.functional_area_code
    functional_areas_parent_functional_area_code document_functional_areas.functional_areas.parent_functional_area_code
    functional_areas_functional_area_description document_functional_areas.functional_areas.functional_area_description
    images_image_id document_sections_images.images.image_id
    images_image_alt_text document_sections_images.images.image_alt_text
    images_image_name document_sections_images.images.image_name
    images_image_url document_sections_images.images.image_url
    documents_document_code document_functional_areas.documents.document_code
    documents_document_type_code document_functional_areas.documents.document_type_code
    documents_access_count document_functional_areas.documents.access_count
    documents_document_name document_functional_areas.documents.document_name
    document_sections_section_id document_sections_images.document_sections.section_id
    document_sections_section_sequence document_sections_images.document_sections.section_sequence
    document_sections_section_code document_sections_images.document_sections.section_code
    document_sections_section_title document_sections_images.document_sections.section_title
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
