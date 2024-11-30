if !isdir("multiple_hypotheses")
    mkdir("multiple_hypotheses")
end

for benchmark in ["rents", "flights", "hospital"]
    # prepare schema, error, and data files 
    custom_schema_file = "src/generator/test1_$(benchmark).txt"
    open(custom_schema_file) do f 
        global text = read(f, String)
    end
    table_json = JSON.parse(split(text, "\n\n")[2])

    custom_error_file = "src/generator/test2_$(benchmark).txt"
    open(custom_error_file) do f 
        global text = read(f, String)
    end
    error_json = JSON.parse(split(text, "\n\n")[2])

    custom_data_file = "datasets/$(test_name)_dirty.csv"
    custom = [custom_schema_file, custom_error_file, custom_data_file]

    ## remove foreign keys from error json -- already represented elsewhere
    if "typos" in keys(error_json)
        foreign_keys = map(t -> t[1], table["foreign_keys"])
        error_json["typos"] = filter(col -> !(findall(tup -> tup[2] == col, table["column_names"])[1] - 1 in foreign_keys), error_json["typos"])
    end

    # 
    has_errors = length(keys(error_json)) != 0
    columns = table_json["column_names"]
    columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
    if has_errors && "unit_errors" in keys(error_json)
        println("YO")
        columns = filter(tup -> !(tup[2] in map(t -> t[1], error_json["unit_errors"])), columns)
    end
    
    class_and_col_index_pairs = []
    for class_index in 1:length(table_json["table_names"])
        columns_of_class = filter(tup -> tup[1] == class_index - 1, columns)
        for col_index in 1:length(columns_of_class) 
            column_name = columns_of_class[col_index][2]
            all_columns_index = findfirst(tup -> tup[1] == class_index - 1 && tup[2] == column_name, table_json["column_names"])
            column_type = table_json["column_types"][all_columns_index]
            if column_type in ["text", "string"]
                push!(class_and_col_index_pairs, (class_index, col_index))
            end
        end
    end

    if length(class_and_col_index_pairs) > 9
        bitstring_len = 9
    else
        bitstring_len = length(class_and_col_index_pairs)
    end

    for i in range(1, 2^bitstring_len)
        bs = reverse(bitstring(i)[end-(bitstring_len - 1):end])
        prior_spec = Dict()
        for i in 1:bitstring_len 
            if bs[i] == "0"
                val = 1
            else
                val = 2
            end
            prior_spec[class_and_col_index_pairs[i]] = val
        end
        
        # generate program and write to file 
        program = generate_program(custom=custom, prior_spec=prior_spec)
        if !isdir("multiple_hypotheses/$(benchmark)")
            mkdir("multiple_hypotheses/$(benchmark)")
        end
        open("multiple_hypotheses/$(benchmark)/format1_$(i).jl", "w") do file 
            write(file, program)
        end

        program2 = generate_program2(custom=custom, prior_spec=prior_spec)
        open("multiple_hypotheses/$(benchmark)/format2_$(i).jl", "w") do file 
            write(file, program2)
        end
    end

    # also write the two original generated programs 
    program = generate_program(custom=custom)
    program2 = generate_program2(custom=custom)

    open("multiple_hypotheses/$(benchmark)/format1_original.jl", "w") do file 
        write(file, program)
    end

    open("multiple_hypotheses/$(benchmark)/format2_original.jl", "w") do file 
        write(file, program2)
    end

end