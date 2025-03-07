using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("staff_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("staff_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "staff id"], Any[0, "gender"], Any[0, "first name"], Any[0, "last name"], Any[0, "email address"], Any[0, "phone number"], Any[1, "customer id"], Any[1, "customer type code"], Any[1, "address line 1"], Any[1, "address line 2"], Any[1, "town city"], Any[1, "state"], Any[1, "email address"], Any[1, "phone number"], Any[2, "product id"], Any[2, "parent product id"], Any[2, "product category code"], Any[2, "date product first available"], Any[2, "date product discontinued"], Any[2, "product name"], Any[2, "product description"], Any[2, "product price"], Any[3, "complaint id"], Any[3, "product id"], Any[3, "customer id"], Any[3, "complaint outcome code"], Any[3, "complaint status code"], Any[3, "complaint type code"], Any[3, "date complaint raised"], Any[3, "date complaint closed"], Any[3, "staff id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "staff id"], Any[0, "gender"], Any[0, "first name"], Any[0, "last name"], Any[0, "email address"], Any[0, "phone number"], Any[1, "customer id"], Any[1, "customer type code"], Any[1, "address line 1"], Any[1, "address line 2"], Any[1, "town city"], Any[1, "state"], Any[1, "email address"], Any[1, "phone number"], Any[2, "product id"], Any[2, "parent product id"], Any[2, "product category code"], Any[2, "date product first available"], Any[2, "date product discontinued"], Any[2, "product name"], Any[2, "product description"], Any[2, "product price"], Any[3, "complaint id"], Any[3, "product id"], Any[3, "customer id"], Any[3, "complaint outcome code"], Any[3, "complaint status code"], Any[3, "complaint type code"], Any[3, "date complaint raised"], Any[3, "date complaint closed"], Any[3, "staff id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "staff id"], Any[0, "gender"], Any[0, "first name"], Any[0, "last name"], Any[0, "email address"], Any[0, "phone number"], Any[1, "customer id"], Any[1, "customer type code"], Any[1, "address line 1"], Any[1, "address line 2"], Any[1, "town city"], Any[1, "state"], Any[1, "email address"], Any[1, "phone number"], Any[2, "product id"], Any[2, "parent product id"], Any[2, "product category code"], Any[2, "date product first available"], Any[2, "date product discontinued"], Any[2, "product name"], Any[2, "product description"], Any[2, "product price"], Any[3, "complaint id"], Any[3, "product id"], Any[3, "customer id"], Any[3, "complaint outcome code"], Any[3, "complaint status code"], Any[3, "complaint type code"], Any[3, "date complaint raised"], Any[3, "date complaint closed"], Any[3, "staff id"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "staff id"], Any[0, "gender"], Any[0, "first name"], Any[0, "last name"], Any[0, "email address"], Any[0, "phone number"], Any[1, "customer id"], Any[1, "customer type code"], Any[1, "address line 1"], Any[1, "address line 2"], Any[1, "town city"], Any[1, "state"], Any[1, "email address"], Any[1, "phone number"], Any[2, "product id"], Any[2, "parent product id"], Any[2, "product category code"], Any[2, "date product first available"], Any[2, "date product discontinued"], Any[2, "product name"], Any[2, "product description"], Any[2, "product price"], Any[3, "complaint id"], Any[3, "product id"], Any[3, "customer id"], Any[3, "complaint outcome code"], Any[3, "complaint status code"], Any[3, "complaint type code"], Any[3, "date complaint raised"], Any[3, "date complaint closed"], Any[3, "staff id"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
## construct possibilities
cols = Any[Any[-1, "*"], Any[0, "staff id"], Any[0, "gender"], Any[0, "first name"], Any[0, "last name"], Any[0, "email address"], Any[0, "phone number"], Any[1, "customer id"], Any[1, "customer type code"], Any[1, "address line 1"], Any[1, "address line 2"], Any[1, "town city"], Any[1, "state"], Any[1, "email address"], Any[1, "phone number"], Any[2, "product id"], Any[2, "parent product id"], Any[2, "product category code"], Any[2, "date product first available"], Any[2, "date product discontinued"], Any[2, "product name"], Any[2, "product description"], Any[2, "product price"], Any[3, "complaint id"], Any[3, "product id"], Any[3, "customer id"], Any[3, "complaint outcome code"], Any[3, "complaint status code"], Any[3, "complaint type code"], Any[3, "date complaint raised"], Any[3, "date complaint closed"], Any[3, "staff id"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[25, 7], Any[24, 15], Any[31, 1]])
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







PClean.@model CustomerComplaintsModel begin
    @class Staff begin
        gender ~ ChooseUniformly(possibilities[:gender])
        first_name ~ ChooseUniformly(possibilities[:first_name])
        last_name ~ ChooseUniformly(possibilities[:last_name])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
    end

    @class Customers begin
        customer_type_code ~ ChooseUniformly(possibilities[:customer_type_code])
        address_line_1 ~ ChooseUniformly(possibilities[:address_line_1])
        address_line_2 ~ ChooseUniformly(possibilities[:address_line_2])
        town_city ~ ChooseUniformly(possibilities[:town_city])
        state ~ ChooseUniformly(possibilities[:state])
        email_address ~ ChooseUniformly(possibilities[:email_address])
        phone_number ~ ChooseUniformly(possibilities[:phone_number])
    end

    @class Products begin
        parent_product_id ~ Unmodeled()
        product_category_code ~ ChooseUniformly(possibilities[:product_category_code])
        date_product_first_available ~ TimePrior(possibilities[:date_product_first_available])
        date_product_discontinued ~ TimePrior(possibilities[:date_product_discontinued])
        product_name ~ ChooseUniformly(possibilities[:product_name])
        product_description ~ ChooseUniformly(possibilities[:product_description])
        product_price ~ ChooseUniformly(possibilities[:product_price])
    end

    @class Complaints begin
        complaint_id ~ Unmodeled()
        products ~ Products
        customers ~ Customers
        complaint_outcome_code ~ ChooseUniformly(possibilities[:complaint_outcome_code])
        complaint_status_code ~ ChooseUniformly(possibilities[:complaint_status_code])
        complaint_type_code ~ ChooseUniformly(possibilities[:complaint_type_code])
        date_complaint_raised ~ TimePrior(possibilities[:date_complaint_raised])
        date_complaint_closed ~ TimePrior(possibilities[:date_complaint_closed])
        staff ~ Staff
    end

    @class Obs begin
        complaints ~ Complaints
    end
end

query = @query CustomerComplaintsModel.Obs [
    staff_id complaints.staff.staff_id
    staff_gender complaints.staff.gender
    staff_first_name complaints.staff.first_name
    staff_last_name complaints.staff.last_name
    staff_email_address complaints.staff.email_address
    staff_phone_number complaints.staff.phone_number
    customers_customer_id complaints.customers.customer_id
    customers_customer_type_code complaints.customers.customer_type_code
    customers_address_line_1 complaints.customers.address_line_1
    customers_address_line_2 complaints.customers.address_line_2
    customers_town_city complaints.customers.town_city
    customers_state complaints.customers.state
    customers_email_address complaints.customers.email_address
    customers_phone_number complaints.customers.phone_number
    products_product_id complaints.products.product_id
    products_parent_product_id complaints.products.parent_product_id
    products_product_category_code complaints.products.product_category_code
    products_date_product_first_available complaints.products.date_product_first_available
    products_date_product_discontinued complaints.products.date_product_discontinued
    products_product_name complaints.products.product_name
    products_product_description complaints.products.product_description
    products_product_price complaints.products.product_price
    complaints_complaint_id complaints.complaint_id
    complaints_complaint_outcome_code complaints.complaint_outcome_code
    complaints_complaint_status_code complaints.complaint_status_code
    complaints_complaint_type_code complaints.complaint_type_code
    complaints_date_complaint_raised complaints.date_complaint_raised
    complaints_date_complaint_closed complaints.date_complaint_closed
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
