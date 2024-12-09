using JSON 
using Statistics 
using CSV
using DataFrames: DataFrame
using DataStructures

tables = JSON.parsefile("src/generator/spider_data/tables.json")
spaces = 4
tab = join(map(x -> " ", 1:spaces))
function generate_program2(table_index=1; random=false, custom=nothing, prior_spec=nothing)
    if !isnothing(custom)
        if length(custom) == 3 
            custom_schema_file, custom_error_file, custom_data_file = custom
            open(custom_schema_file) do f 
                global text = read(f, String)
            end
            table = JSON.parse(split(text, "\n\n")[2])
    
            open(custom_error_file) do f 
                global text = read(f, String)
            end
            error_json = JSON.parse(split(text, "\n\n")[2])
        else
            table, error_json = custom
            custom_data_file = "spider_dataset_excerpts/$(table["db_id"])_excerpt.csv"
        end

        # remove foreign keys from error json -- already represented elsewhere
        if "typos" in keys(error_json)
            foreign_keys = map(t -> t[1], table["foreign_keys"])
            error_json["typos"] = filter(col -> !(findall(tup -> tup[2] == col, table["column_names"])[1] - 1 in foreign_keys), error_json["typos"])
        end

        # data handling
        dirty_table = CSV.File(custom_data_file) |> DataFrame

        omitted = []
        if length(names(dirty_table)) != length(table["column_names"])
            for dirty_name in names(dirty_table)
                if !isnothing(custom) && length(custom) == 2 
                    if !(dirty_name in map(tup -> tup[2], table["column_names"]))
                        push!(omitted, dirty_name)
                    end
                else
                    if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), table["column_names"]))
                        push!(omitted, dirty_name)
                    end
                end
            end
        end
        dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
        
        ## construct possibilities
        foreign_keys = map(tup -> table["column_names"][tup[1] + 1][2], table["foreign_keys"])
        if length(custom) == 2 
            cols = map(tup -> tup[2], table["column_names"])
            column_renaming_dict = Dict(zip(cols, cols))
            column_renaming_dict_reverse = Dict(zip(cols, cols))
        else
            column_names_without_foreign_keys = filter(tup -> !(tup in foreign_keys), table["column_names"])
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
        end
        println(column_renaming_dict)
        println(dirty_columns)

        possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
        for r in eachrow(dirty_table)
            for col in dirty_columns
                if !ismissing(r[col]) 
                    push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
                end
            end
        end
        possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))
        # println("possibilities woo")
        # println(possibilities)
        manual_join = false
        clean_table = CSV.File(replace("$(custom_data_file)", "dirty.csv" => "clean.csv")) |> DataFrame
        if "ProviderNumber" in names(clean_table)
            println("EXCUSE ME")
            clean_table_modification = """clean_table[!, :PhoneNumber] = map(x -> "\$x", clean_table[!, :PhoneNumber])
    clean_table[!, :ZipCode] = map(x -> "\$x", clean_table[!, :ZipCode])
    clean_table[!, :ProviderNumber] = map(x -> "\$x", clean_table[!, :ProviderNumber])"""
        else 
            clean_table_modification = ""
        end
    else
        if !random 
            table = tables[table_index]
        else
            table = tables[rand(1:length(tables))]
        end
        table_json["table_names"] = map(x -> replace(x, " " => "_"), table_json["table_names"])
        error_json = JSON.parse("""{"swaps" : [["name", ["home_town"], "height"]]}""")
        possibilities = JSON.parse("{}")
        # error_json = JSON.parse("""{"swaps" : [["weight", ["killed"], "height"]]}""")
        possibilities = Dict([:name => ["100", "200"], :height => [68, 72], :home_town => ["20", "20", "30"]])
        
        # remove foreign keys from error json -- already represented elsewhere
        if "typos" in keys(error_json)
            foreign_keys = map(t -> t[1], table["foreign_keys"])
            error_json["typos"] = filter(col -> !(findall(tup -> tup[2] == col, table["column_names"])[1] - 1 in foreign_keys), error_json["typos"])
        end
        foreign_keys = map(tup -> table["column_names"][tup[1] + 1][2], table["foreign_keys"])

        custom_data_file = table["table_names"][1] * "_dirty.csv"

        dirty_table_names = map(t -> t[2], table["column_names"])
        dirty_table = DataFrame(columns=dirty_table_names)
        cleaned_names = map(t -> replace(t[2], " " => "_"), table["column_names"])
        column_renaming_dict = Dict(zip(dirty_table_names, cleaned_names))
        column_renaming_dict_reverse = Dict(zip(cleaned_names, dirty_table_names))
        manual_join = true
        clean_table_modification = ""
    end

    model_name = join(map(x -> capitalize(x), split(table["db_id"], "_")), "")

    if length(keys(error_json)) != 0 && "unit_errors" in keys(error_json)
        scale_factors = unique([1, map(tup -> tup[2], error_json["unit_errors"])...])
        transformations = map(x -> "Transformation(x -> x/$(x)* 1.0, x -> x*$(x)*1.0, x -> 1/$(x)*1.0)", scale_factors)
        units_str = """units = [$(join(transformations, ", "))]"""
    else
        units_str = ""
    end

    if !isnothing(custom) && length(custom) == 2 
        renaming_dict_str = """omitted = []
        if length(names(dirty_table)) != length($(table["column_names"]))
            for dirty_name in names(dirty_table)
                if !(dirty_name in map(tup -> tup[2], $(table["column_names"])))
                    push!(omitted, dirty_name)
                end
            end
        end
        dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
        ## construct possibilities
        cols = $(map(tup -> tup[2], table["column_names"]))
        column_renaming_dict = Dict(zip(cols, cols))
        column_renaming_dict_reverse = Dict(zip(cols, cols))"""
    else
        renaming_dict_str = """omitted = []
        if length(names(dirty_table)) != length($(table["column_names"]))
            for dirty_name in names(dirty_table)
                if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), $(table["column_names"])))
                    push!(omitted, dirty_name)
                end
            end
        end
        dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
        ## construct possibilities
        cols = $(table["column_names"])
        foreign_keys = map(tup -> cols[tup[1] + 1], $(table["foreign_keys"]))
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
        column_renaming_dict_reverse = Dict(zip(matching_columns, dirty_columns))"""
    end

    swap_possibilities_str = ""
    if length(keys(error_json)) != 0 && "swaps" in keys(error_json)
        swap_possibilities_str = """swap_possibilities = Dict()
        swap_columns = $(error_json["swaps"])
        for swap_column in swap_columns
            swap_column_name = swap_column[1]
            same_identity_column_name = swap_column[2][1] 
            for r in eachrow(dirty_table)
                col_val = r[same_identity_column_name]
                swap_val = r[column_renaming_dict_reverse[swap_column_name]]
                key = "\$(col_val)-\$(swap_column_name)"
                if !ismissing(swap_val)
                    if !(key in keys(swap_possibilities))
                        swap_possibilities[key] = Set()
                    end
                    push!(swap_possibilities[key], swap_val)
                end
            end
        end
        swap_possibilities = Dict(c => [swap_possibilities[c]...] for c in keys(swap_possibilities))"""
    end

    subset_size_string = """subset_size = size(dirty_table, 1)
    dirty_table = first(dirty_table, subset_size)
    clean_table = first(clean_table, subset_size)"""

    return """using PClean
    using CSV
    using DataFrames: DataFrame
    using Statistics
    
    # data handling
    dirty_table = CSV.File("$(custom_data_file)") |> DataFrame
    clean_table = CSV.File(replace("$(custom_data_file)", "dirty.csv" => "clean.csv")) |> DataFrame
    $(clean_table_modification)

    $(swap_possibilities_str != "" ? "" : subset_size_string)

    omitted = []
    if length(names(dirty_table)) != length($(table["column_names"]))
        for dirty_name in names(dirty_table)
            if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), $(table["column_names"])))
                push!(omitted, dirty_name)
            end
        end
    end
    dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
    
    ## construct possibilities
    $(renaming_dict_str)

    possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
    for r in eachrow(dirty_table)
        for col in dirty_columns
            if !ismissing(r[col]) 
                push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
            end
        end
    end
    possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))
    
    $(swap_possibilities_str)

    $(units_str)

    $(swap_possibilities_str == "" ? "" : subset_size_string)

    PClean.@model $(model_name)Model begin
    $(generate_classes2(table, error_json, possibilities, prior_spec, custom))
    end

    $(generate_query2(table, error_json, column_renaming_dict_reverse, manual_join))

    observations = [ObservedDataset(query, dirty_table)]
    config = PClean.InferenceConfig($(size(dirty_table, 1) > 10000 ? 1 : 5), 2; use_mh_instead_of_pg=true)
    @time begin 
        tr = initialize_trace(observations, config);
        run_inference!(tr, config)
    end

    println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
    """
end

function generate_classes2(table_json, error_json, possibilities, custom_priors, custom)
    has_errors = length(keys(error_json)) != 0

    class_strings = []
    obs_class_declaration_strings = []
    non_primary_classes = map(tup -> table_json["table_names"][table_json["column_names"][tup[1] + 1][1] + 1], table_json["foreign_keys"])
    while length(class_strings) != length(table_json["table_names"])
        println("LENGTH?")
        println(length(class_strings))
        for (index, class) in enumerate(table_json["table_names"])

            formatted_class = join(map(x -> capitalize(x), split(class, " ")), "_")
            unformatted_class = lowercase(formatted_class[1:1]) * formatted_class[2:end]
            # push!(obs_class_declaration_strings, "$(tab)$(tab)$(unformatted_class) ~ $(formatted_class)")
            println("formatted_class")
            println(formatted_class)
            println(class_strings)
            if occursin("@class $(formatted_class)", join(class_strings, "")) 
                continue
            end
            # don't include unmodeled first column
            columns = table_json["column_names"]
            if !(!isnothing(custom) && length(custom) == 2)
                unmodeled_columns = filter(tup -> occursin("id", tup[2]) && table_json["column_types"][findall(x -> x == tup, table_json["column_names"])[1]] in ["number", "integer"] && tup in map(i -> table_json["column_names"][i + 1], table_json["primary_keys"]), table_json["column_names"])
                columns = filter(tup -> !(tup in unmodeled_columns), table_json["column_names"])            
                # columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
            end
            # println(columns)
            if has_errors && "unit_errors" in keys(error_json)
                println("YO")
                columns = filter(tup -> !(tup[2] in map(t -> t[1], error_json["unit_errors"])), columns)
            end

            foreign_key_tuples = map(tup -> table_json["column_names"][tup[1] + 1], table_json["foreign_keys"])
            formatted_columns = []
            precendent_missing = false
            for tup in enumerate(filter(x -> x[1] == index - 1, columns))
                if tup[2] in foreign_key_tuples
                    col_index = findall(x -> x == tup[2], table_json["column_names"])[1]
                    foreign_col_index = 1 + filter(pair -> pair[1] == col_index - 1, table_json["foreign_keys"])[1][2]
                    foreign_table_index = table_json["column_names"][foreign_col_index][1] + 1
                    foreign_class = table_json["table_names"][foreign_table_index]
                    formatted_foreign_class = join(map(x -> capitalize(x), split(foreign_class, " ")), "_")
                    unformatted_foreign_class = lowercase(formatted_foreign_class[1:1]) * formatted_foreign_class[2:end]
                    
                    # check if precedent class has already been constructed
                    if !occursin("@class $(formatted_foreign_class)", join(class_strings, "")) 
                        precendent_missing = true
                        break
                    end

                    formatted_col = """$(tab)$(tab)$(unformatted_foreign_class) ~ $(formatted_foreign_class)"""
                    push!(formatted_columns, formatted_col)
                else
                    formatted_col = """$(tab)$(tab)$(replace(tup[2][2], " " => "_")) ~ $(generate_prior2(table_json, index, tup[1], error_json, possibilities, custom, custom_priors))"""
                    push!(formatted_columns, formatted_col)
                end
            end

            if precendent_missing 
                continue
            end

            class_str = """$(tab)@class $(formatted_class) begin
        $(join(unique(formatted_columns), "\n"))
        $(tab)end"""
            push!(class_strings, class_str)
            
        end
    end

    # generate Obs class 
    ## classes to declare in Obs are those without any foreign key references to them 
    foreign_tables = map(tup -> table_json["table_names"][table_json["column_names"][tup[2] + 1][1] + 1], table_json["foreign_keys"])
    for class in table_json["table_names"]
        if !(class in foreign_tables)
            formatted_class = join(map(x -> capitalize(x), split(class, " ")), "_")
            unformatted_class = lowercase(formatted_class[1:1]) * formatted_class[2:end]
            push!(obs_class_declaration_strings, "$(tab)$(tab)$(unformatted_class) ~ $(formatted_class)")
        end
    end
    obs_column_strings = obs_class_declaration_strings

    if "swaps" in keys(error_json)
        variation_columns = unique(map(tup -> tup[3], error_json["swaps"]))
        for variation_column in variation_columns
            if variation_column != ""
                # println("variation_column")
                # println(variation_column)
                # create a class for the variation column
                formatted_class = join(map(x -> capitalize(x), split(variation_column, " ")), "_")

                println(columns)
                if !(!isnothing(custom) && length(custom) == 2)
                    unmodeled_columns = filter(tup -> occursin("id", tup[2]) && table_json["column_types"][findall(x -> x == tup, table_json["column_names"])[1]] in ["number", "integer"] && tup in map(i -> table_json["column_names"][i + 1], table_json["primary_keys"]), table_json["column_names"])
                    columns = filter(tup -> !(tup in unmodeled_columns), table_json["column_names"])
                    # columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
                end
                if has_errors && "unit_errors" in keys(error_json)
                    println("YO")
                    columns = filter(tup -> !(tup[2] in map(t -> t[1], error_json["unit_errors"])), table_json["column_names"])
                end

                println("WHAT")
                println(columns)
                println(variation_column)
                variation_column_index = findall(tup -> tup[2] == variation_column, columns)[1]
                class_index = columns[variation_column_index][1]
                variation_column_index = findall(tup -> tup[2] == variation_column, filter(t -> t[1] == class_index, columns))[1]
                println("variation_column_index")
                println(variation_column_index)
                println("class_index")
                println(class_index)
                prior = "$(variation_column) ~ $(generate_prior2(table_json, class_index + 1, variation_column_index, error_json, possibilities, custom, custom_priors))"
                class_str = """$(tab)@class $(formatted_class) begin
                $(tab)$(tab)$(prior)
                $(tab)end"""
                # println(class_strings)
                # println(prior)
                if filter(s -> occursin(prior, s), class_strings) != [] 
                    old_class_str = filter(s -> occursin(prior, s), class_strings)[1]
                    fixed_class_str = replace(replace(old_class_str, prior => ";"), "$(tab)$(tab);\n" => "")
                    class_strings = filter(s -> !occursin(prior, s), class_strings)
                    class_strings = [class_str, fixed_class_str, class_strings...]
                end

                error_decl_str = "$(tab)$(tab)@learned error_probs::Dict{String, ProbParameter{10.0, 50.0}}"    
                error_def_str = "$(tab)$(tab)error_prob_$(variation_column) = error_probs[$(variation_column).$(variation_column)]"
                obs_column_strings = [error_decl_str, obs_column_strings...]
                push!(obs_column_strings, error_def_str)
            else
                error_def_str = "$(tab)$(tab)error_prob_$(variation_column) = 1e-5"
                push!(obs_column_strings, error_def_str)
            end
        end
    end

    if "swaps" in keys(error_json)
        variation_columns = unique(filter(x -> x != "", map(tup -> tup[3], error_json["swaps"])))
    else
        variation_columns = []
    end

    if !(!isnothing(custom) && length(custom) == 2)
        unmodeled_columns = filter(tup -> occursin("id", tup[2]) && table_json["column_types"][findall(x -> x == tup, table_json["column_names"])[1]] in ["number", "integer"] && tup in map(i -> table_json["column_names"][i + 1], table_json["primary_keys"]), table_json["column_names"])
        columns = filter(tup -> !(tup in unmodeled_columns), table_json["column_names"])    
        # columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
    end
    # println(columns)
    if has_errors && "unit_errors" in keys(error_json)
        println("YO")
        columns = filter(tup -> !(tup[2] in map(t -> t[1], error_json["unit_errors"])), columns)
    end

    handled = Dict()
    loops = 0
    while length(keys(handled)) != length(table_json["column_names"])
        loops += 1 
        println("loops")
        println(loops)
        for (index, column) in enumerate(table_json["column_names"])
            # keep column as long as it's not the FK in a FK-PK pair
            println(column)
            if column[1] != -1 && !((index - 1) in map(tup -> tup[1], table_json["foreign_keys"]))
                println(column)
                original_table_name = table_json["table_names"][column[1] + 1]
                table_name = join(map(x -> capitalize(x), split(table_json["table_names"][column[1] + 1], " ")), "_")
                table_name = lowercase(table_name[1:1]) * table_name[2:end]
                original_column_name = column[2]
                column_name = replace(original_column_name, " " => "_")

                if occursin("id", column_name) && table_json["column_types"][index] in ["number", "integer"] && [column[1], column_name] in map(i -> table_json["column_names"][i + 1], table_json["primary_keys"])
                    handled[string(index) * "_" * string(column[2])] = true
                    continue
                end

                # if !occursin(lowercase(table_name), lowercase(column_name))
                #     column_name = "$(lowercase(table_name))_$(column_name)"
                # end

                if column_name in variation_columns
                    formatted_column_name = join(map(x -> capitalize(x), split(column_name, " ")), "_")
                    obs_column_str = "$(tab)$(tab)$(column_name) ~ $(formatted_column_name)"
                    obs_column_strings = [obs_column_strings[1], obs_column_str, obs_column_strings[2:end]...]
                    handled[string(index) * "_" * string(column[2])] = true
                    continue
                else
                    if has_errors 
                        if "typos" in keys(error_json) && original_column_name in error_json["typos"]
                            println(original_column_name)
                            obs_column_str = "$(tab)$(tab)$(column_name) ~ AddTypos($(compute_table_prefix(index, table_json)).$(column_name), 2)"
                        elseif "unit_errors" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["unit_errors"])
                            avg = Statistics.mean(possibilities[Symbol(column_name)])
                            st_dev = Statistics.std(possibilities[Symbol(column_name)])
                            learned_param_str = "@learned avg_$(column_name)::Dict{String, MeanParameter{$(avg), $(st_dev)}}"
                            unit_str = "unit_$(column_name) ~ ChooseUniformly(units)"
                            # TODO: need to ensure that these are declared first! and swap column is declared first in below case!
                            text_column_names = map(x -> table_json["column_names"][x[1]], filter(tup -> tup[2] == "text" && table_json["column_names"][tup[1]][1] == column[1], [enumerate(table_json["column_types"])...]))
                            formatted_text_column_names = join(map(tup -> """\$($(compute_table_prefix(index, table_json)).$(replace(tup[2], " " => "_")))""", text_column_names), "_")
                            base_str = """$(column_name)_base = avg_$(column_name)["$(formatted_text_column_names)"]"""
                            error_str = """$(column_name) ~ TransformedGaussian($(column_name)_base, $(st_dev)/10, unit_$(column_name))"""
                            corrected_str = "$(column_name)_corrected = round(unit_$(column_name).backward($(column_name)))"
                            obs_column_str = join(map(line -> "$(tab)$(tab)$(line)", [learned_param_str, unit_str, base_str, error_str, corrected_str]), "\n")
                        elseif "swaps" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["swaps"])
                            swap_column_name = filter(tup -> tup[1] == original_column_name, error_json["swaps"])[1][2][1] 
                            variation_column_name = filter(tup -> tup[1] == original_column_name, error_json["swaps"])[1][3] 
                            k = string(index) * "_" * swap_column_name
                            
                            obs_column_str = """$(tab)$(tab)$(column_name) ~ MaybeSwap($(compute_table_prefix(index, table_json)).$(column_name), swap_possibilities["\$($(compute_table_prefix(index, table_json)).$(swap_column_name))-$(column_name)"], error_prob_$(variation_column_name))"""
                        else 
                            handled[string(index) * "_" * string(column[2])] = true
                            continue
                            # obs_column_str = "$(tab)$(tab)$(column_name) ~ $(table_name).$(column_name)"
                        end
                    else
                        handled[string(index) * "_" * string(column[2])] = true
                        continue
                        # obs_column_str = "$(tab)$(tab)$(column_name) ~ $(table_name).$(column_name)"
                    end
                end
                handled[string(index) * "_" * string(column[2])] = true
                push!(obs_column_strings, obs_column_str)
            else
                handled[string(index) * "_" * string(column[2])] = true
            end
        end
    end
    class_str = """$(tab)@class Obs begin
    $(join(obs_column_strings, "\n"))
    $(tab)end"""
    push!(class_strings, class_str)

    join(class_strings, "\n\n")
end

function compute_table_prefix(column_index, table_json)
    println("compute_table_prefix")
    println("column_index")
    println(column_index)
    table_index = table_json["column_names"][column_index][1] + 1
    table_name = table_json["table_names"][table_index]
    foreign_tables = map(tup -> table_json["table_names"][table_json["column_names"][tup[2] + 1][1] + 1], table_json["foreign_keys"])
    top_level_tables = filter(t -> !(t in foreign_tables), table_json["table_names"])
    if table_name in top_level_tables 
        table_name = join(map(x -> capitalize(x), split(table_name, " ")), "_")
        table_name = lowercase(table_name[1:1]) * table_name[2:end]
        return table_name
    else # find path to table_name, starting at top-most tables
        table_path = Dict()
        queue = Queue{String}()
        enqueue!(queue, table_name)
        while !(first(queue) in top_level_tables)
            println("yo")
            curr_table = dequeue!(queue)
            curr_table_index = findall(x -> x == curr_table, table_json["table_names"])[1] - 1
            curr_table_col_indices = filter(tup -> tup[1] == curr_table_index, table_json["column_names"])
            curr_table_col_indices = findall(x -> x in curr_table_col_indices, table_json["column_names"])
            referring_foreign_key_pairs = filter(tup -> tup[2] + 1 in curr_table_col_indices, table_json["foreign_keys"])
            
            for pair in referring_foreign_key_pairs 
                referencing_column_index = pair[1] + 1
                referencing_table_index = table_json["column_names"][referencing_column_index][1] + 1
                referencing_table = table_json["table_names"][referencing_table_index]
                enqueue!(queue, referencing_table)
                table_path[referencing_table] = curr_table
            end
        end
        curr_table = first(queue)
        table_list = [curr_table]
        while curr_table != table_name 
            curr_table = table_path[curr_table]
            push!(table_list, curr_table)
        end
        formatted_table_names = map(table_name -> join(map(x -> capitalize(x), split(table_name, " ")), "_"), table_list)
        formatted_table_names = map(table_name -> lowercase(table_name[1:1]) * table_name[2:end], formatted_table_names)        
        return join(formatted_table_names, ".")
    end
end

function generate_query2(table_json, error_json, column_renaming_dict_reverse, manual_join)
    has_errors = length(keys(error_json)) != 0
    query_column_strings = []
    non_primary_classes = map(tup -> table_json["table_names"][table_json["column_names"][tup[1] + 1][1] + 1], table_json["foreign_keys"])
    for (index, column) in enumerate(table_json["column_names"])
        # keep column as long as it's not the FK in a FK-PK pair
        if column[1] != -1 && !((index - 1) in map(tup -> tup[1], table_json["foreign_keys"]))

            original_table_name = table_json["table_names"][column[1] + 1]
            table_name = join(map(x -> capitalize(x), split(original_table_name, " ")), "_")
            table_name = lowercase(table_name[1:1]) * table_name[2:end]
            original_column_name = column[2]
            column_name = replace(original_column_name, " " => "_")

            # don't include unmodeled first column
            if occursin("id", column_name) && table_json["column_types"][index] in ["number", "integer"] && [column[1], column_name] in map(i -> table_json["column_names"][i + 1], table_json["primary_keys"])
                continue
            end

            if manual_join # length(table_json["table_names"]) > 1 # manual joining case -- we change all the column names
                if !occursin(lowercase(table_name), lowercase(column_name))
                    query_column_name = "$(lowercase(table_name))_$(column_name)"
                else
                    query_column_name = column_name
                end
            else 
                println(column_renaming_dict_reverse)
                query_column_name = column_renaming_dict_reverse[column_name]
                if occursin(" ", query_column_name)
                    query_column_name = "\"" * query_column_name * "\""
                end
            end

            ## compute "." path to foreign key reference
            table_name = compute_table_prefix(index, table_json)

            if has_errors 
                if "typos" in keys(error_json) && original_column_name in error_json["typos"]
                    obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name) $(column_name)"
                elseif "unit_errors" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["unit_errors"])
                    obs_column_str = "$(tab)$(query_column_name) $(column_name)_corrected $(column_name)"
                elseif "swaps" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["swaps"])
                    obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name) $(column_name)"
                elseif "swaps" in keys(error_json) && original_column_name in map(tup -> tup[3], error_json["swaps"])
                    obs_column_str = "$(tab)$(query_column_name) $(column_name).$(column_name)"
                else
                    obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name)"
                end
            else 
                obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name)"
            end
            push!(query_column_strings, obs_column_str)
        end
    end
    model_name = join(map(x -> capitalize(x), split(table_json["db_id"], "_")), "")
    query_str = """query = @query $(model_name)Model.Obs [
    $(join(query_column_strings, "\n"))
    ]
    """
end

function generate_prior2(table_json, class_index, col_index, error_json, ps, custom, custom_priors=nothing)
    class = table_json["table_names"][class_index]

    has_errors = length(keys(error_json)) != 0
    columns = table_json["column_names"]
    if !(!isnothing(custom) && length(custom) == 2)
        unmodeled_columns = filter(tup -> occursin("id", tup[2]) && table_json["column_types"][findall(x -> x == tup, table_json["column_names"])[1]] in ["number", "integer"] && tup in map(i -> table_json["column_names"][i + 1], table_json["primary_keys"]), table_json["column_names"])
        columns = filter(tup -> !(tup in unmodeled_columns), table_json["column_names"])    
        # columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
    end
    if has_errors && "unit_errors" in keys(error_json)
        println("YO")
        columns = filter(tup -> !(tup[2] in map(t -> t[1], error_json["unit_errors"])), columns)
    end
    println(columns)
    println(class_index)
    println(col_index)
    column_name = filter(tup -> tup[1] == class_index - 1, columns)[col_index][2]

    all_columns_index = findfirst(tup -> tup[1] == class_index - 1 && tup[2] == column_name, table_json["column_names"])
    
    column_type = table_json["column_types"][all_columns_index]
    
    formatted_column_name = replace(column_name, " " => "_")

    if col_index == 1 && occursin("id", column_name) && column_type in ["number", "integer"]
        return "Unmodeled()"
    end
    
    if custom_priors != nothing && (class_index, col_index) in keys(custom_priors)
        option_number = custom_priors[(class_index, col_index)]
        if option_number == 1 
            return "ChooseUniformly(possibilities[:$(formatted_column_name)])"
        elseif option_number == 2
            if Symbol(column_name) in keys(ps)
                max_length = maximum(map(x -> length(x), ps[Symbol(column_name)]))
                min_length = minimum(map(x -> length(x), ps[Symbol(column_name)]))
                return "StringPrior($(min_length), $(max_length), possibilities[:$(formatted_column_name)])"
            else
                return "StringPrior(5, 35, possibilities[:$(formatted_column_name)])"
            end
        end
    end

    if column_type == "text"
        if Symbol(column_name) in keys(ps) && length(ps[Symbol(column_name)]) > 100 
            options = ["StringPrior(5, 35, possibilities[:$(formatted_column_name)])"]
        else
            options = ["ChooseUniformly(possibilities[:$(formatted_column_name)])"]
        end
    elseif column_type == "time"
        if "swaps" in keys(error_json) && column_name in map(tup -> tup[1], error_json["swaps"])
            swap_column_name = filter(tup -> tup[1] == column_name, error_json["swaps"])[1][2][1] 
            options = ["""TimePrior(swap_possibilities["\$($(swap_column_name))-$(formatted_column_name)"])"""]
        else
            options = ["TimePrior(possibilities[:$(formatted_column_name)])"]
        end
    else
        options = ["ChooseUniformly(possibilities[:$(formatted_column_name)])"]
    end

    return rand(options)
end

function capitalize(str)
    if str == ""
        return str
    end

    return uppercase(str[1:1]) * str[2:end]
end


# for i in 1:length(tables)
#     println("GENERATING")
#     println(i)
#     t = tables[i]
#     name = join(map(x -> capitalize(x), split(t["db_id"], "_")), "")
#     open("src/generator/generated_programs2/$(string(i))_$(name).jl", "w+") do file
#         write(file, generate_program2(i))
#     end
# end

# test_name = ARGS[1]

# custom_schema_file = "src/generator/test1_$(test_name).txt"
# custom_error_file = "src/generator/test2_$(test_name).txt"
# custom_data_file = "datasets/$(test_name)_dirty.csv"
# custom = [custom_schema_file, custom_error_file, custom_data_file]

# program = generate_program(custom=custom)
# println(program)
# open("output_$(test_name).jl", "w") do file 
#     write(file, program)
# end

# """
# TODO: goal is to generate programs from spider benchmark.
# - clean_table modification won't work for spider benchmark (DONE)
# - spider benchmark and pclean benchmark schema jsons use slightly
# indexes for foreign keys. 

# """