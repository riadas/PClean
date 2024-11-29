using JSON 
using Statistics 
using CSV
using DataFrames: DataFrame


tables = JSON.parsefile("src/generator/spider_data/tables.json")
spaces = 4
tab = join(map(x -> " ", 1:spaces))
function generate_program(table_index=1; random=false, custom=nothing)
    if !isnothing(custom)
        custom_schema_file, custom_error_file, custom_data_file = custom
        open(custom_schema_file) do f 
            global text = read(f, String)
        end
        table = JSON.parse(split(text, "\n\n")[2])

        open(custom_error_file) do f 
            global text = read(f, String)
        end
        error_json = JSON.parse(split(text, "\n\n")[2])

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
                if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), table["column_names"]))
                    push!(omitted, dirty_name)
                end
            end
        end
        dirty_columns = filter(n -> !(n in omitted), names(dirty_table))
        
        ## construct possibilities
        foreign_keys = map(tup -> table["column_names"][tup[1] + 1][2], table["foreign_keys"])
        column_names_without_foreign_keys = filter(tup -> !(tup[2] in foreign_keys), table["column_names"])
        if length(omitted) == 0 
            column_renaming_dict = Dict(zip(dirty_columns, map(t -> t[2], column_names_without_foreign_keys)))
            column_renaming_dict_reverse = Dict(zip(map(t -> t[2], column_names_without_foreign_keys), dirty_columns))
        else
            column_renaming_dict = Dict(zip(sort(dirty_columns), sort(map(t -> t[2], column_names_without_foreign_keys))))
            column_renaming_dict_reverse = Dict(zip(sort(map(t -> t[2], column_names_without_foreign_keys)), sort(dirty_columns)))    
        end

        possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
        for r in eachrow(dirty_table)
            for col in dirty_columns
                if !ismissing(r[col]) 
                    push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
                end
            end
        end
        possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))
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
        error_json = JSON.parse("{}")
        # error_json = JSON.parse("""{"swaps" : [["weight", ["killed"], "height"]]}""")
        # possibilities = Dict([:weight => [100, 200], :height => [68, 72], :killed => [20, 20, 30]])
        possibilities = Dict()
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

    return """using PClean
    using CSV
    using DataFrames: DataFrame
    using Statistics
    
    # data handling
    dirty_table = CSV.File("$(custom_data_file)") |> DataFrame
    clean_table = CSV.File(replace("$(custom_data_file)", "dirty.csv" => "clean.csv")) |> DataFrame
    $(clean_table_modification)

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
    foreign_keys = $(map(tup -> table["column_names"][tup[1] + 1][2], table["foreign_keys"]))
    column_names_without_foreign_keys = $(filter(tup -> !(tup[2] in foreign_keys), table["column_names"]))
    if length(omitted) == 0 
        column_renaming_dict = Dict(zip(dirty_columns, map(t -> t[2], column_names_without_foreign_keys)))
        column_renaming_dict_reverse = Dict(zip(map(t -> t[2], column_names_without_foreign_keys), dirty_columns))
    else
        column_renaming_dict = Dict(zip(sort(dirty_columns), sort(map(t -> t[2], column_names_without_foreign_keys))))
        column_renaming_dict_reverse = Dict(zip(sort(map(t -> t[2], column_names_without_foreign_keys)), sort(dirty_columns)))    
    end

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

    PClean.@model $(model_name)Model begin
    $(generate_classes(table, error_json, possibilities))
    end

    $(generate_query(table, error_json, column_renaming_dict_reverse, manual_join))

    observations = [ObservedDataset(query, dirty_table)]
    config = PClean.InferenceConfig($(size(dirty_table, 1) > 10000 ? 1 : 5), 2; use_mh_instead_of_pg=true)
    @time begin 
        tr = initialize_trace(observations, config);
        run_inference!(tr, config)
    end

    println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
    """
end

function generate_classes(table_json, error_json, possibilities)
    has_errors = length(keys(error_json)) != 0

    class_strings = []
    obs_class_declaration_strings = []
    non_primary_classes = map(tup -> table_json["table_names"][table_json["column_names"][tup[1] + 1][1] + 1], table_json["foreign_keys"])
    for (index, class) in enumerate(table_json["table_names"])

        if !(class in non_primary_classes)
            formatted_class = join(map(x -> capitalize(x), split(class, " ")), "_")
            unformatted_class = lowercase(formatted_class[1:1]) * formatted_class[2:end]
            push!(obs_class_declaration_strings, "$(tab)$(tab)$(unformatted_class) ~ $(formatted_class)")
    
            # don't include unmodeled first column
            columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
            # println(columns)
            if has_errors && "unit_errors" in keys(error_json)
                println("YO")
                columns = filter(tup -> !(tup[2] in map(t -> t[1], error_json["unit_errors"])), columns)
            end
    
            formatted_columns = map(tup -> """$(tab)$(tab)$(replace(tup[2][2], " " => "_")) ~ $(generate_prior(table_json, index, tup[1], error_json, possibilities))""", enumerate(filter(x -> x[1] == index - 1, columns)))
    
            class_str = """$(tab)@class $(formatted_class) begin
        $(join(formatted_columns, "\n"))
        $(tab)end"""
            push!(class_strings, class_str)
        end
    end

    # generate Obs class 
    ## generate Obs columns: union of all columns minus join columns
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
                columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
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
                prior = "$(variation_column) ~ $(generate_prior(table_json, class_index + 1, variation_column_index, error_json, possibilities))"
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

    columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
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

                if index == 1 && occursin("id", column_name) && table_json["column_types"][index] in ["number", "integer"]
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
                            if original_table_name in non_primary_classes 
                                class_index = column[1] + 1
                                col_index = findall(t -> t[2] == original_column_name, filter(tup -> tup[1] == class_index - 1, columns))[1]    
                                obs_column_str = """$(tab)$(tab)$(column_name) ~ $(generate_prior(table_json, class_index, col_index, error_json, possibilities))"""
                                obs_err_str = "$(tab)$(tab)$(column_name)_typo ~ AddTypos($(column_name), 2)"
                                obs_column_str = join([obs_column_str, obs_err_str], "\n")
                            else
                                obs_column_str = "$(tab)$(tab)$(column_name) ~ AddTypos($(table_name).$(column_name), 2)"
                            end
                        elseif "unit_errors" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["unit_errors"])
                            avg = Statistics.mean(possibilities[Symbol(column_name)])
                            st_dev = Statistics.std(possibilities[Symbol(column_name)])
                            learned_param_str = "@learned avg_$(column_name)::Dict{String, MeanParameter{$(avg), $(st_dev)}}"
                            unit_str = "unit_$(column_name) ~ ChooseUniformly(units)"
                            # TODO: need to ensure that these are declared first! and swap column is declared first in below case!
                            text_column_names = map(x -> table_json["column_names"][x[1]], filter(tup -> tup[2] == "text" && table_json["column_names"][tup[1]][1] == column[1], [enumerate(table_json["column_types"])...]))
                            for tup in text_column_names
                                k = string(tup[1]) * "_" * string(tup[2])
                                if !(k in keys(handled)) && (original_table_name in non_primary_classes)
                                    continue
                                end
                            end
                            formatted_text_column_names = join(map(tup -> table_json["table_names"][tup[1] + 1] in non_primary_classes ? """\$($(replace(tup[2], " " => "_")))""" : """\$($(table_name).$(replace(tup[2], " " => "_")))""", text_column_names), "_")
                            base_str = """$(column_name)_base = avg_$(column_name)["$(formatted_text_column_names)"]"""
                            error_str = """$(column_name) ~ TransformedGaussian($(column_name)_base, $(st_dev)/10, unit_$(column_name))"""
                            corrected_str = "$(column_name)_corrected = round(unit_$(column_name).backward($(column_name)))"
                            obs_column_str = join(map(line -> "$(tab)$(tab)$(line)", [learned_param_str, unit_str, base_str, error_str, corrected_str]), "\n")
                        elseif "swaps" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["swaps"])
                            swap_column_name = filter(tup -> tup[1] == original_column_name, error_json["swaps"])[1][2][1] 
                            variation_column_name = filter(tup -> tup[1] == original_column_name, error_json["swaps"])[1][3] 
                            k = string(index) * "_" * swap_column_name
                            if (original_table_name in non_primary_classes) && !(k in keys(handled))
                                continue
                            end
                            
                            if original_table_name in non_primary_classes  
                                obs_column_str = """$(tab)$(tab)$(column_name)_swap ~ MaybeSwap($(column_name), swap_possibilities["\$($(swap_column_name))-$(column_name)"], error_prob_$(variation_column_name))"""
                            else
                                obs_column_str = """$(tab)$(tab)$(column_name) ~ MaybeSwap($(table_name).$(column_name), swap_possibilities["\$($(table_name).$(swap_column_name))-$(column_name)"], error_prob_$(variation_column_name))"""                                
                            end
                        elseif original_table_name in non_primary_classes
                            # no error, but this field still needs to be instantiated b/c it is not in a primary table
                            class_index = column[1] + 1
                            col_index = findall(t -> t[2] == original_column_name, filter(tup -> tup[1] == class_index - 1, columns))[1]
                            obs_column_str = """$(tab)$(tab)$(column_name) ~ $(generate_prior(table_json, class_index, col_index, error_json, possibilities))"""
                        else 
                            handled[string(index) * "_" * string(column[2])] = true
                            continue
                            # obs_column_str = "$(tab)$(tab)$(column_name) ~ $(table_name).$(column_name)"
                        end
                    elseif original_table_name in non_primary_classes
                        class_index = column[1] + 1
                        col_index = findall(t -> t[2] == original_column_name, filter(tup -> tup[1] == class_index - 1, columns))[1]
                        obs_column_str = """$(tab)$(tab)$(column_name) ~ $(generate_prior(table_json, class_index, col_index, error_json, possibilities))"""
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

function generate_query(table_json, error_json, column_renaming_dict_reverse, manual_join)
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

            # don't include unmodeled first column
            if occursin("id", column_name) && table_json["column_types"][index] in ["number", "integer"] && table_json["column_names"][1][2] == column_name
                continue
            end

            if has_errors 
                if "typos" in keys(error_json) && original_column_name in error_json["typos"]
                    if original_table_name in non_primary_classes 
                        obs_column_str = "$(tab)$(query_column_name) $(column_name) $(column_name)_typo"
                    else
                        obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name) $(column_name)"
                    end
                elseif "unit_errors" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["unit_errors"])
                    obs_column_str = "$(tab)$(query_column_name) $(column_name)_corrected $(column_name)"
                elseif "swaps" in keys(error_json) && original_column_name in map(tup -> tup[1], error_json["swaps"])
                    if original_table_name in non_primary_classes
                        obs_column_str = "$(tab)$(query_column_name) $(column_name) $(column_name)_swap"
                    else
                        obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name) $(column_name)"
                    end
                elseif "swaps" in keys(error_json) && original_column_name in map(tup -> tup[3], error_json["swaps"])
                    obs_column_str = "$(tab)$(query_column_name) $(column_name).$(column_name)"
                else
                    obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name)"
                end
            else 
                if original_table_name in non_primary_classes 
                    obs_column_str = "$(tab)$(query_column_name) $(column_name)"
                else
                    obs_column_str = "$(tab)$(query_column_name) $(table_name).$(column_name)"
                end
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

function generate_prior(table_json, class_index, col_index, error_json, ps)
    class = table_json["table_names"][class_index]

    has_errors = length(keys(error_json)) != 0
    columns = table_json["column_names"]
    columns = map(x -> x[2], filter(tup -> !(tup[1] == 1 && occursin("id", tup[2][2]) && table_json["column_types"][tup[1]] in ["number", "integer"]), [enumerate(table_json["column_names"])...]))
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
#     println(i)
#     t = tables[i]
#     name = join(map(x -> capitalize(x), split(t["db_id"], "_")), "")
#     open("src/generator/generated_programs/$(string(i))_$(name).jl", "w+") do file
#         write(file, generate_program(i))
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