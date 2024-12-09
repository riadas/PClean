using JSON 
for method in ["direct_synthesis"] # ["synthesis_from_jsons", "direct_synthesis"]
    for benchmark in ["rents", "hospital"]
        base_files = readdir("multiple_hypotheses_base/inputs/$(method)/$(benchmark)")
        num_bases = length(filter(filename -> occursin("test1", filename), base_files))
        for base_index in 1:num_bases 
            # prepare schema, error, and data files 
            custom_schema_file = "multiple_hypotheses_base/inputs/$(method)/$(benchmark)/test1_$(benchmark)_$(base_index).txt"
            open(custom_schema_file) do f 
                global text = read(f, String)
            end
            table_json = JSON.parse(split(text, "\n\n")[2])

            custom_error_file = "multiple_hypotheses_base/inputs/$(method)/$(benchmark)/test2_$(benchmark)_$(base_index).txt"
            open(custom_error_file) do f 
                global text = read(f, String)
            end
            error_json = JSON.parse(split(text, "\n\n")[2])

            custom_data_file = "datasets/$(benchmark)_dirty.csv"
            custom = [custom_schema_file, custom_error_file, custom_data_file]

            ## remove foreign keys from error json -- already represented elsewhere
            if "typos" in keys(error_json)
                foreign_keys = map(t -> t[1], table_json["foreign_keys"])
                error_json["typos"] = filter(col -> !(findall(tup -> tup[2] == col, table_json["column_names"])[1] - 1 in foreign_keys), error_json["typos"])
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

            if length(class_and_col_index_pairs) > 7
                bitstring_len = 7
            else
                bitstring_len = length(class_and_col_index_pairs)
            end

            for i in range(0, 2^bitstring_len - 1)
                println("generating $(i) for $(benchmark)")
                if bitstring_len == length(class_and_col_index_pairs)
                    bs = reverse(bitstring(i)[end-(bitstring_len - 1):end])
                    prior_spec = Dict()
                    println(bs)
                    for j in 1:bitstring_len 
                        if bs[j] == '0'
                            val = 1
                        else
                            val = 2
                        end
                        prior_spec[class_and_col_index_pairs[j]] = val
                    end
                else
                    full_length = length(class_and_col_index_pairs)
                    j = rand(1:2^full_length - 1)
                    bs = reverse(bitstring(j)[end-(full_length - 1):end])
                    prior_spec = Dict()
                    for k in 1:full_length
                        if bs[k] == '0'
                            val = 1
                        else
                            val = 2
                        end
                        prior_spec[class_and_col_index_pairs[k]] = val
                    end
                end
                println("prior_spec")
                println(prior_spec)
                
                # generate program and write to file 
                program = generate_program(custom=custom, prior_spec=prior_spec)
                if !isdir("multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)")
                    mkdir("multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)")
                end
                open("multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/format1_base$(base_index)_$(i).jl", "w") do file 
                    write(file, program)
                end

                program2 = generate_program2(custom=custom, prior_spec=prior_spec)
                open("multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/format2_base$(base_index)_$(i).jl", "w") do file 
                    write(file, program2)
                end
            end

            # also write the two original generated programs 
            program = generate_program(custom=custom)
            program2 = generate_program2(custom=custom)

            open("multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/format1_original.jl", "w") do file 
                write(file, program)
            end

            open("multiple_hypotheses_base/hypotheses/$(method)/$(benchmark)/format2_original.jl", "w") do file 
                write(file, program2)
            end
        end
    end
end