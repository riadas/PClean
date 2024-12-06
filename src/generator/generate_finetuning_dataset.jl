home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
using DataStructures
using StatsBase 
using Random

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

        if error_type == "typo"
            
            if "typos" in keys(error_json) 
                columns = filter(tup -> table_json["column_types"][findall(t -> t == tup, table_json["column_names"])[1]] in ["text", "string"], columns)
                column_name = rand(columns)[2]
                push!(error_json["typos"], column_name)
            else
                column_name = rand(columns)[2]
                error_json["typos"] = [column_name]
            end

        elseif error_type == "unit_error"
            units = [1000, 100, 10, 2.2, 2.54]
            columns = filter(tup -> !(table_json["column_types"][findall(t -> t == tup, table_json["column_names"])[1]] in ["text", "string", "time"]), columns)
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
                table_index = rand(table_indices_with_enough_columns)
                columns = filter(tup -> tup[1] == table_index - 1, columns)
                swap_columns = sample(columns, 3, replace = false)
                swap_columns = shuffle(swap_columns)
                error_json["swaps"] = [[swap_columns[1], [swap_columns[2]], swap_columns[3]]]
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
            sentence = "Sometimes the $(tup[1]) value for a given $(tup[2]) value are different across different $(tup[3]) values, when they should be the same. It should be inferred which of these is correct so all $(tup[3]) values provide the same information."
            push!(sentences, sentence)
        end
    end

    join(sentences, " ")
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
    repeat_column_names = Set(unique(map(x -> replace(x[2], " " => "_"), filter(tup -> count(t -> t[2] == tup[2], table_json["column_names"]) > 1, table_json["column_names"]))))
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

    return dataset_excerpt, subgraph_table_indices, subgraph_pk_fk_pairs
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

function generate_prompts_and_completions()

end