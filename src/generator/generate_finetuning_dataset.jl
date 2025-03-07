home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
using DataStructures
using StatsBase 
using Random
using JSON

function generate_error_json(table_json)
    num_errors = rand(1:3)
    error_json = Dict()
    for error_index in 1:num_errors
        if "swaps" in keys(error_json)
            error_type = rand(["typo", "unit_error"])
        else
            error_type = rand(["typo", "unit_error", "swap"])
        end

        foreign_keys = map(tup -> table_json["column_names"][tup[1] + 1], table_json["foreign_keys"])
        columns = filter(tup -> !(tup in foreign_keys), table_json["column_names"])
        if "unit_errors" in keys(error_json)
            columns = filter(tup -> !(tup[2] in map(x -> x[1], error_json["unit_errors"])), columns)
        end

        if "typos" in keys(error_json) 
            columns = filter(tup -> !(tup[2] in error_json["typos"]), columns)
        end

        if "swaps" in keys(error_json)
            columns = filter(tup -> !(tup[2] in [error_json["swaps"][1][1], error_json["swaps"][1][2][1], error_json["swaps"][1][3]]), columns)
        end

        if error_type == "typo"

            columns = filter(tup -> table_json["column_types"][findall(t -> t == tup, table_json["column_names"])[1]] in ["text", "string"], columns)
            if length(columns) == 0 
                continue
            end
            if "typos" in keys(error_json)
                columns = filter(tup -> !(tup[1] in error_json["typos"]), columns)
                if length(columns) == 0 
                    continue
                end
                column_name = rand(columns)[2]
                push!(error_json["typos"], column_name)
            else
                column_name = rand(columns)[2]
                error_json["typos"] = [column_name]
            end

        elseif error_type == "unit_error"
            units = [1000, 100, 10, 2.2, 2.54]
            columns = filter(tup -> !(table_json["column_types"][findall(t -> t == tup, table_json["column_names"])[1]] in ["text", "string", "time"]), columns)
            if length(columns) == 0 
                continue
            end
            column_name = rand(columns)[2]
            unit = rand(units)
            
            val = [column_name, unit]
            if "unit_errors" in keys(error_json)
                push!(error_json["unit_errors"], val)
            else
                error_json["unit_errors"] = [val]
            end

        elseif error_type == "swap"
            table_indices_with_enough_columns = filter(i -> length(filter(tup -> tup[1] == i - 1, columns)) >= 3, 1:length(table_json["table_names"]))
            if length(table_indices_with_enough_columns) != 0 
                if length(columns) == 0 
                    continue
                end
                if length(table_indices_with_enough_columns) == 0 
                    continue
                end
                table_index = rand(table_indices_with_enough_columns)
                columns = filter(tup -> tup[1] == table_index - 1, columns)
                swap_columns = sample(columns, 3, replace = false)
                swap_columns = shuffle(swap_columns)
                error_json["swaps"] = [[swap_columns[1][2], [swap_columns[2][2]], swap_columns[3][2]]]
            end
        end
    end
    return error_json
end

function generate_mechanical_error_description(error_json)
    sentences = []

    for error_type in keys(error_json)
        if error_type == "typos"
            sentence = """There are typos in the $(format_list(error_json["typos"])) column$(length(error_json["typos"]) > 1 ? "s" : "")."""
            push!(sentences, sentence)
        elseif error_type == "unit_errors"
            for tup in error_json["unit_errors"]
                sentence = "Sometimes the $(tup[1]) value is incorrectly reported in $(tup[2]) times the correct unit."
                push!(sentences, sentence)
            end
        elseif error_type == "swaps"
            tup = error_json["swaps"][1]
            sentence = "Sometimes the $(tup[1]) value for a given $(tup[2][1]) value are different across different $(tup[3]) values, when they should be the same. It should be inferred which of these is correct so all $(tup[3]) values provide the same information."
            push!(sentences, sentence)
        end
    end

    join(sentences, " ")
end

function generate_mechanical_schema_description(table_json)
    sentences = []
    sentence = """The schema has $(length(table_json["table_names"])) tables, which we'll call $(format_list(table_json["table_names"]))."""
    push!(sentences, sentence)
    for table_index in 1:length(table_json["table_names"])
        table_name = table_json["table_names"][table_index]
        table_columns = map(t -> t[2], filter(tup -> tup[1] == table_index - 1, table_json["column_names"]))
        sentence = "The table $(table_name) has columns $(format_list(table_columns))."
        push!(sentences, sentence)
    end
    foreign_key_phrases = []
    for pair in table_json["foreign_keys"]
        associated_column_tuples = map(i -> table_json["column_names"][i + 1], pair)
        associated_table_names = map(tup -> table_json["table_names"][tup[1] + 1], associated_column_tuples)
        associated_column_names = map(tup -> tup[2], associated_column_tuples)
        phrase = "$(associated_column_names[1]) of $(associated_table_names[1]) is a foreign key reference to $(associated_column_names[2]) of $(associated_table_names[2])"
        push!(foreign_key_phrases, phrase)
    end
    sentence = "Finally, $(format_list(foreign_key_phrases))."
    push!(sentences, sentence)
    return join(sentences, " ")
end

function format_list(list)
    if length(list) == 0
        return ""
    elseif length(list) == 1
        return string(list[1])
    elseif length(list) == 2
        return "$(list[1]) and $(list[2])"
    else
        comma_separated = join(list[1:end-1], ", ")
        return "$(comma_separated), and $(list[end])"
    end
end

function massage_mechanical_error_description(desc)
    # make a call to an LLM, asking it to rephrase the robotic error description to sound more natural

end

function generate_dataset_excerpt(table_json, dataset_size=40, random_subset=false)
    # write SQL code to run
    subgraph_table_indices, subgraph_pk_fk_pairs, _ = find_largest_subgraph(table_json)

    if random_subset
        subset_size = rand(1:length(subgraph_table_indices))
        subgraph_table_indices = subgraph_table_indices[1:subset_size]
        println("random subset size: $(subset_size)")
        println("pre-random subset")
        println(subgraph_table_indices)
        println(subgraph_pk_fk_pairs)
    
        # filter pk-fk pairs to only those related to the above tables
        new_pk_fk_pairs = []
        for pair in subgraph_pk_fk_pairs 
            table_indices = map(x -> table_json["column_names"][x + 1][1] + 1, pair)
            if table_indices[1] in subgraph_table_indices && table_indices[2] in subgraph_table_indices 
                push!(new_pk_fk_pairs, pair)
            end
        end
        subgraph_pk_fk_pairs = new_pk_fk_pairs
    end
    println("post-random subset")
    println(subgraph_table_indices)
    println(subgraph_pk_fk_pairs)

    table_names = map(i -> table_json["table_names_original"][i], subgraph_table_indices)
    join_statements = []
    added_tables = [subgraph_table_indices[1]]
    handled_table_pairs = Dict()
    for pair in subgraph_pk_fk_pairs 
        println(pair)
        println(keys(handled_table_pairs))
        println(added_tables)
        println(map(i -> table_json["table_names_original"][i], added_tables))
        column_from_new_table = filter(x -> !(table_json["column_names"][x + 1][1] + 1 in added_tables), pair)
        if length(column_from_new_table) > 0
            column_from_new_table = column_from_new_table[1]
            new_table = table_json["column_names"][column_from_new_table + 1][1]
            push!(added_tables, new_table + 1)
            new_table_name = table_json["table_names_original"][new_table + 1]
            old_table = table_json["column_names"][filter(x -> x != column_from_new_table, pair)[1] + 1][1]
            old_table_name = table_json["table_names_original"][old_table + 1]
    
            if occursin(" ", new_table_name) 
                new_table_name = "\"$(table_name)\""
            end
    
            if occursin(" ", old_table_name) 
                old_table_name = "\"$(table_name)\""
            end
    
            new_table_column = table_json["column_names_original"][column_from_new_table + 1][2]
            column_from_old_table = filter(x -> x != column_from_new_table, pair)[1]
            old_table_column = table_json["column_names_original"][column_from_old_table + 1][2]
    
            if occursin(" ", new_table_column)
                new_table_column = "\"$(new_table_column)\""
            end
    
            if occursin(" ", old_table_column)
                old_table_column = "\"$(new_table_column)\""
            end
    
            join_statement = "JOIN $(new_table_name) ON $(old_table_name).$(old_table_column) = $(new_table_name).$(new_table_column)"
            push!(join_statements, join_statement)
            sorted_table_pair = sort([old_table_name, new_table_name])
            println(sorted_table_pair)
            handled_table_pairs[Tuple(sorted_table_pair)] = join_statement
        else # both columns are from tables that have been previously handled 
            table_names_ = map(x -> table_json["table_names_original"][table_json["column_names"][x + 1][1] + 1], pair)            
            formatted_table_names = map(name -> occursin(" ", name) ? "\"$(name)\"" : name, table_names_)
            column_names = map(x -> table_json["column_names_original"][x + 1][2], pair)
            formatted_column_names = map(name -> occursin(" ", name) ? "\"$(name)\"" : name, column_names)
            
            

            sorted_table_names = sort(formatted_table_names)
            if (Tuple(sorted_table_names) in keys(handled_table_pairs))
                old_join_statement = handled_table_pairs[Tuple(sorted_table_names)]
                
                prefixed_table1 = "$(formatted_table_names[1]).$(formatted_column_names[1])"
                prefixed_table2 = "$(formatted_table_names[2]).$(formatted_column_names[2])"

                if occursin(prefixed_table1, old_join_statement) || occursin(prefixed_table2, old_join_statement)
                    continue
                end

                conjunction = "AND $(prefixed_table1) = $(prefixed_table2)"
                new_join_statement = "$(old_join_statement) $(conjunction)"
    
                join_statements = filter(s -> s != old_join_statement, join_statements)
                push!(join_statements, new_join_statement)
                handled_table_pairs[Tuple(sorted_table_names)] = new_join_statement
            else 
                # find the table in the pair that is added last, and modify its 
                table_indices = map(x -> table_json["column_names"][x + 1][1] + 1, pair) 
                added_indices = map(i -> findall(x -> x == i, added_tables)[1], table_indices)            
                later_table_index = filter(i -> added_indices[i] == maximum(added_indices), 1:length(table_indices))[1]
                if later_table_index == 1
                    reversed = true
                else
                    reversed = true
                end
                
                later_table_index = table_indices[later_table_index]
                earlier_table_index = filter(x -> x != later_table_index, table_indices)[1]

                # find join statement where later table was added
                later_table_name = table_json["table_names_original"][later_table_index]
                formatted_later_table_name = occursin(" ", later_table_name) ? "\"$(later_table_name)\"" : later_table_name
                old_join_statement = filter(s -> occursin("JOIN $(formatted_later_table_name)", s), join_statements)[1]
                key = filter(k -> handled_table_pairs[k] == old_join_statement, [keys(handled_table_pairs)...])[1]

                earlier_table_name = table_json["table_names_original"][earlier_table_index]
                formatted_earlier_table_name = occursin(" ", earlier_table_name) ? "\"$(earlier_table_name)\"" : earlier_table_name

                column_names = map(x -> table_json["column_names_original"][x + 1][2], pair)
                formatted_column_names = map(name -> occursin(" ", name) ? "\"$(name)\"" : name, column_names)
                if reversed 
                    formatted_column_names = reverse(formatted_column_names)
                end

                conjunction = "AND $(earlier_table_name).$(formatted_column_names[1]) = $(later_table_name).$(formatted_column_names[2])"
                new_join_statement = "$(old_join_statement) $(conjunction)"
                join_statements = filter(s -> s != old_join_statement, join_statements)
                push!(join_statements, new_join_statement)
                handled_table_pairs[key] = new_join_statement
            end
        end
    end
    join_statements_str = join(join_statements, "\n")
    columns_list = []
    columns_after_subset = filter(tup -> (tup[1] + 1) in subgraph_table_indices, table_json["column_names"])
    repeat_column_names = Set(unique(map(x -> replace(x[2], " " => "_"), filter(tup -> count(t -> t[2] == tup[2], columns_after_subset) > 1, columns_after_subset))))
    for table_index in subgraph_table_indices 
        table_name = table_json["table_names_original"][table_index]

        if occursin(" ", table_name)
            table_name = "\"$(table_name)\""
        end

        table_columns = filter(tup -> tup[1] == table_index - 1, table_json["column_names_original"])
        table_columns = map(tup -> occursin(" ", tup[2]) ? [tup[1], "\"$(tup[2])\""] : tup, table_columns)

        aliases = filter(tup -> tup[1] == table_index - 1, table_json["column_names"])
        aliases = map(tup -> replace(tup[2], " " => "_"), aliases)
        formatted_aliases = []
        alias_table_name = replace(table_json["table_names"][table_index], " " => "_")
        for alias in aliases
            if alias in repeat_column_names && !occursin(alias_table_name, alias)
                push!(formatted_aliases, "$(alias_table_name)_$(alias)")
            else
                push!(formatted_aliases, alias)
            end
        end
        column_names = map(tup -> "$(table_name).$(tup[1][2]) AS $(tup[2])", zip(table_columns, formatted_aliases))
        push!(columns_list, column_names...)
    end

    column_names_to_remove = []
    for pair in subgraph_pk_fk_pairs 
        column_tup = table_json["column_names_original"][pair[1] + 1]
        column_name = column_tup[2]
        if occursin(" ", column_name) 
            column_name = "\"$(column_name)\""
        end
        table_name = table_json["table_names_original"][column_tup[1] + 1]
        if occursin(" ", table_name) 
            table_name = "\"$(table_name)\""
        end
        alias = replace(table_json["column_names"][pair[1] + 1][2], " " => "_")
        alias_table_name = replace(table_json["table_names"][column_tup[1] + 1], " " => "_")
        if alias in repeat_column_names 
            alias = "$(alias_table_name)_$(alias)"
        end
        push!(column_names_to_remove, "$(table_name).$(column_name) AS $(alias)")
    end

    columns_list = filter(col -> !(col in column_names_to_remove), columns_list)

    sql_command = """SELECT $(join(columns_list, ", ")) FROM 
    $(table_names[1]) $(join_statements_str) 
    LIMIT $(dataset_size)
    """
    shell_script = """#!/bin/bash
    sqlite3 $(home_directory)/spider_data/database/$(table_json["db_id"])/$(table_json["db_id"]).sqlite <<EOF
    .headers on
    .mode csv
    $(sql_command)
    EOF"""

    open("test.sh", "w") do f
        write(f, shell_script)
    end
    
    # run SQL code
    dataset_excerpt = readchomp(`bash test.sh`)
    
    # write excerpt to file
    if !isdir("spider_dataset_excerpts")
        mkdir("spider_dataset_excerpts")
    end
    open("""spider_dataset_excerpts/$(table_json["db_id"])_excerpt.csv""", "w") do f 
        write(f, dataset_excerpt)
    end

    return dataset_excerpt, subgraph_table_indices, subgraph_pk_fk_pairs
end

# used to generate schema JSON / PClean programs with subsets of the full schema
function update_schema_json_with_subset(table_json, subgraph_table_indices, subgraph_pk_fk_pairs)
    println("update_schema_json_with_subset")
    println(subgraph_pk_fk_pairs)
    # to update: table_names (incl. original), column_names (incl. original), column_types, foreign_keys
    new_table_json = deepcopy(table_json)
    subgraph_table_indices = sort(subgraph_table_indices)

    # new table_names and table_names_original: just filter to subgraph_table_indices
    old_to_new_table_indices = Dict(map(tup -> tup[1] - 1 => tup[2], zip(subgraph_table_indices, [0:(length(subgraph_table_indices) - 1)...])))
    new_table_names = map(i -> table_json["table_names"][i], subgraph_table_indices)
    new_table_names_original = map(i -> table_json["table_names_original"][i], subgraph_table_indices)

    # new column_types: just filter to columns in subgraph_table_indices
    column_positions_to_keep = findall(tup -> (tup[1] + 1) in subgraph_table_indices, table_json["column_names"])
    new_column_types = map(i -> table_json["column_types"][i], column_positions_to_keep)

    # new column_names and column_names_original: filter to columns with tables in subgraph_table_indices, then remap table indices
    new_column_indices = [0:(length(column_positions_to_keep) - 1)...]
    old_to_new_column_indices = Dict(map(tup -> tup[1] - 1 => tup[2], zip(column_positions_to_keep, new_column_indices)))

    new_column_names = deepcopy(map(i -> table_json["column_names"][i], column_positions_to_keep))
    new_column_names_original = deepcopy(map(i -> table_json["column_names_original"][i], column_positions_to_keep))

    for i in 1:length(new_column_names)
        # update associated table indices
        new_column_names[i][1] = old_to_new_table_indices[new_column_names[i][1]]
        new_column_names_original[i][1] = old_to_new_table_indices[new_column_names_original[i][1]]
    end

    # new foreign_keys: use column index remapping dictionary to remap indices
    println(subgraph_pk_fk_pairs)
    new_foreign_keys = []
    for pair in subgraph_pk_fk_pairs
        new_pair = [old_to_new_column_indices[pair[1]], old_to_new_column_indices[pair[2]]]
        push!(new_foreign_keys, new_pair)
    end

    # need to rename columns and tables
    new_table_names = map(t -> replace(t, " " => "_"), new_table_names)
    new_column_names = map(tup -> [tup[1], replace(tup[2], " " => "_")], new_column_names)
    repeat_column_names = Set(unique(map(x -> replace(x[2], " " => "_"), filter(tup -> count(t -> t[2] == tup[2], new_column_names) > 1, new_column_names))))
    new_column_names_formatted = []
    for tup in new_column_names 
        if tup[2] in repeat_column_names 
            table_name = new_table_names[tup[1] + 1]
            if !occursin(table_name, tup[2])
                formatted_col_name = "$(table_name)_$(tup[2])"
                push!(new_column_names_formatted, [tup[1], formatted_col_name])
            else
                push!(new_column_names_formatted, tup)
            end 
        else
            push!(new_column_names_formatted, tup)
        end
    end

    # new primary_keys: filter out only those in the new table set, and then remap with column index remap dictionary
    new_primary_keys = filter(col_index -> (table_json["column_names"][col_index + 1][1] + 1) in subgraph_table_indices, table_json["primary_keys"])
    new_primary_keys = map(i -> old_to_new_column_indices[i], new_primary_keys)

    new_table_json["table_names"] = new_table_names 
    new_table_json["table_names_original"] = new_table_names_original
    new_table_json["column_names"] = new_column_names_formatted 
    new_table_json["column_names_original"] = new_column_names_original
    new_table_json["foreign_keys"] = new_foreign_keys
    new_table_json["column_types"] = new_column_types
    new_table_json["primary_keys"] = new_primary_keys

    return new_table_json
end

function find_largest_subgraph(table_json)
    subgraph_tables = Dict()
    subgraph_pk_fk_pairs = Dict()
    unhandled_tables = Dict(map(x -> x => true, 1:length(table_json["table_names"])))
    unhandled_pk_fk_pairs = Dict(map(x -> Tuple(x) => true, table_json["foreign_keys"]))
    
    while length(keys(unhandled_tables)) != 0 
        table_index = minimum([keys(unhandled_tables)...])
        delete!(unhandled_tables, table_index)

        subgraph_id = length(keys(subgraph_tables)) + 1
        subgraph_tables[subgraph_id] = []
        subgraph_pk_fk_pairs[subgraph_id] = []

        queue = Queue{Integer}()
        enqueue!(queue, table_index)
        while !isempty(queue)
            curr_table = dequeue!(queue)
            push!(subgraph_tables[subgraph_id], curr_table)
            delete!(unhandled_tables, curr_table)
            # find all pk-fk pairs containing this table, if it's not yet handled 
            pk_fk_pairs = filter(tup -> (table_json["column_names"][tup[1] + 1][1] == curr_table - 1 || table_json["column_names"][tup[2] + 1][1] == curr_table - 1) && (Tuple(tup) in keys(unhandled_pk_fk_pairs)), table_json["foreign_keys"]) 
            next_tables = unique(map(t -> table_json["column_names"][t + 1][1], filter(x -> table_json["column_names"][x + 1][1] != curr_table - 1, vcat(pk_fk_pairs...))))
            for next_table in next_tables
                enqueue!(queue, next_table + 1)
            end
            for pair in pk_fk_pairs 
                delete!(unhandled_pk_fk_pairs, Tuple(pair))
                push!(subgraph_pk_fk_pairs[subgraph_id], pair)
            end
        end
    end
    
    largest_subgraph_id = -1
    largest_subgraph_size = 0
    for id in keys(subgraph_tables)
        if largest_subgraph_id == -1 
            largest_subgraph_id = id 
            largest_subgraph_size = length(subgraph_tables[id])
        elseif length(subgraph_tables[id]) > largest_subgraph_size
            largest_subgraph_id = id 
            largest_subgraph_size = length(subgraph_tables[id])
        end
    end

    fully_connected = largest_subgraph_size == length(table_json["table_names"])
    return (unique(subgraph_tables[largest_subgraph_id]), subgraph_pk_fk_pairs[largest_subgraph_id], fully_connected)
end

function is_fully_connected(table_json)
    _, _, fully_connected = find_largest_subgraph(table_json)
    return fully_connected
end

function test_generation(table_json, subset_bool=false)
    dataset_excerpt, subgraph_table_indices, subgraph_pk_fk_pairs = generate_dataset_excerpt(table_json, 40, subset_bool)
    updated_schema_json = update_schema_json_with_subset(table_json, subgraph_table_indices, subgraph_pk_fk_pairs)
    error_json = generate_error_json(updated_schema_json)
    error_description = generate_mechanical_error_description(error_json)
    println("error_json")
    println(error_json)
    program = generate_program2(custom=[updated_schema_json, error_json])
    return (dataset_excerpt, updated_schema_json, error_json, error_description, program)
end

function test_generation_conversation_json(table_json, mode="conversational_terse", subset_bool=false)
    dataset_excerpt, schema_json, error_json, error_description, program = test_generation(table_json, subset_bool)
    program = "PClean.@model$(split(program, "PClean.@model")[end])"
    if mode == "conversational_terse" 
        user_message = """PClean is a domain-specific language for dataset cleaning. In the following, I will provide an excerpt of a dataset to be cleaned, as well as a description of errors found in the dataset, and will ask you to write a PClean program that describes the schema of the dataset and its contained errors, which can be run to automatically fix the errors. 
    
        Dataset Excerpt:
        $(dataset_excerpt)
    
        Error Description: $(error_description)
    
        Given this dataset excerpt and error description, please respond with a PClean program that describes the schema of the datase and the errors it contains. In your response, don't include any other text other than the PClean program.
        """
        llm_response = program
        
        messages = [
            """{"role": "system", "content": "You are a programmer helping write code from user instructions."}""",
            """{"role": "user", "content": $(repr(user_message))}""",
            """{"role": "assistant", "content": $(repr(llm_response))}""",
        ]
        output_json = """{"messages": [$(join(messages, ","))]}"""
        return output_json
    elseif mode == "conversational_long"
        user_message = """PClean is a domain-specific language for dataset cleaning. In the following, I will provide an excerpt of a dataset to be cleaned, as well as a description of errors found in the dataset, and will ask you to write a PClean program that describes the schema of the dataset and its contained errors, which can be run to automatically fix the errors. 
    
        Dataset Excerpt:
        $(dataset_excerpt)
    
        Error Description: $(error_description)
    
        Given this dataset excerpt and error description, the goal is to write a PClean program that describes the schema of the dataset and the errors it contains. 

        A good first step is to describe the schema of the dataset, since the structure of the PClean program closely follows the dataset schema. Can you describe the schema of the given dataset, provided the excerpt above?
        """
        llm_response = program
        schema_description = generate_mechanical_schema_description(schema_json)
        
        messages = [
            """{"role": "system", "content": "You are a programmer helping write code from user instructions."}""",
            """{"role": "user", "content": $(repr(user_message))}""",
            """{"role": "assistant", "content": "Yes! The following describes the schema: $(schema_description)"}""",
            """{"role": "user", "content": "Awesome! Now, based on this schema and the previously provided error description, please write a PClean program that describes the schema of the dataset and the errors it contains. Please only include the PClean program in your response, without any preceding or following text."}""",
            """{"role": "assistant", "content": $(repr(llm_response))}"""            
        ]
        output_json = """{"messages": [$(join(messages, ","))]}"""
        return output_json
    elseif mode == "instruct"
        user_message = """PClean is a domain-specific language for dataset cleaning. In the following, I will provide an excerpt of a dataset to be cleaned, as well as a description of errors found in the dataset, and will ask you to write a PClean program that describes the schema of the dataset and its contained errors, which can be run to automatically fix the errors. 
    
        Dataset Excerpt:
        $(dataset_excerpt)
    
        Error Description: $(error_description)
    
        Given this dataset excerpt and error description, please respond with a PClean program that describes the schema of the datase and the errors it contains. In your response, don't include any other text other than the PClean program.
        """
        llm_response = program
        return """{"prompt": $(repr(user_message)), "completion": $(repr(llm_response))}"""
    end
end

function demo_generation_conversation_json(table_json, mode="conversational_terse", subset_bool=false)
    dataset_excerpt, schema_json, error_json, error_description, program = test_generation(table_json, subset_bool)
    program = "PClean.@model$(split(program, "PClean.@model")[end])"
    if mode == "conversational_terse" 
        user_message = """PClean is a domain-specific language for dataset cleaning. In the following, I will provide an excerpt of a dataset to be cleaned, as well as a description of errors found in the dataset, and will ask you to write a PClean program that describes the schema of the dataset and its contained errors, which can be run to automatically fix the errors. 
    
        Dataset Excerpt:
        $(dataset_excerpt)
    
        Error Description: $(error_description)
    
        Given this dataset excerpt and error description, please respond with a PClean program that describes the schema of the datase and the errors it contains. In your response, don't include any other text other than the PClean program.
        """
        llm_response = program
        
        messages = [
            """{"role": "system", "content": "You are a programmer helping write code from user instructions."}""",
            """{"role": "user", "content": "$(user_message)"}""",
            """{"role": "assistant", "content": "$(llm_response)"}""",
        ]
        output_json = """{"messages": [$(join(messages, ",\n"))]}"""
        return output_json
    elseif mode == "conversational_long"
        user_message = """PClean is a domain-specific language for dataset cleaning. In the following, I will provide an excerpt of a dataset to be cleaned, as well as a description of errors found in the dataset, and will ask you to write a PClean program that describes the schema of the dataset and its contained errors, which can be run to automatically fix the errors. 
    
        Dataset Excerpt:
        $(dataset_excerpt)
    
        Error Description: $(error_description)
    
        Given this dataset excerpt and error description, the goal is to write a PClean program that describes the schema of the dataset and the errors it contains. 

        A good first step is to describe the schema of the dataset, since the structure of the PClean program closely follows the dataset schema. Can you describe the schema of the given dataset, provided the excerpt above?
        """
        llm_response = program
        schema_description = generate_mechanical_schema_description(schema_json)
        
        messages = [
            """{"role": "system", "content": "You are a programmer helping write code from user instructions."}""",
            """{"role": "user", "content": "$(user_message)"}""",
            """{"role": "assistant", "content": "Yes! The following describes the schema: $(schema_description)"}""",
            """{"role": "user", "content": "Awesome! Now, based on this schema and the previously provided error description, please write a PClean program that describes the schema of the dataset and the errors it contains. Please only include the PClean program in your response, without any preceding or following text."}""",
            """{"role": "assistant", "content": "$(llm_response)"}"""            
        ]
        output_json = """{"messages": [$(join(messages, ",\n"))]}"""
        return output_json
    elseif mode == "instruct"
        user_message = """PClean is a domain-specific language for dataset cleaning. In the following, I will provide an excerpt of a dataset to be cleaned, as well as a description of errors found in the dataset, and will ask you to write a PClean program that describes the schema of the dataset and its contained errors, which can be run to automatically fix the errors. 
    
        Dataset Excerpt:
        $(dataset_excerpt)
    
        Error Description: $(error_description)
    
        Given this dataset excerpt and error description, please respond with a PClean program that describes the schema of the datase and the errors it contains. In your response, don't include any other text other than the PClean program.
        """
        llm_response = program
        return """{"prompt": "$(user_message)",\n"completion": "$(llm_response)"}"""
    end
end

function generate_prompts_and_completions(mode="conversational_long", num_subsets=0)
    finetuning_jsons = []
    tables = JSON.parsefile("src/generator/spider_data/tables.json")
    skip = [41, 59, 95, 107, 113, 129]
    error_tables = []
    # first generate full joins
    for i in 11:length(tables) # 1:length(tables)
        if !(i in skip)
            table_json = tables[i] 
            try
                finetuning_json = test_generation_conversation_json(table_json, mode)
                push!(finetuning_jsons, finetuning_json)
            catch e 
                push!(error_tables, i)
            end
        end
    end

    # generate subsets
    for i in 11:length(tables) # 1:length(tables)
        table_json = tables[i]
        if !(i in skip)
            for j in 1:num_subsets 
                try
                    finetuning_json = test_generation_conversation_json(table_json, mode, true)
                    push!(finetuning_jsons, finetuning_json)
                catch e 
                    push!(error_tables, i)
                end 
            end
        end
    end

    finetuning_jsons = unique(finetuning_jsons)
    error_tables = unique(error_tables)


    filename = "dataset_mode_$(mode)_num_subsets_$(num_subsets).jsonl"
    open("finetuning_datasets/$(filename)", "w") do f 
        write(f, join(finetuning_jsons, "\n"))
    end
    output = readchomp(`python $(home_directory)/src/generator/format_finetuning_dataset.py $(filename)`)
    println(output)
    return error_tables
end