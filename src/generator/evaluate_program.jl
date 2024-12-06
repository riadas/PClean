using PClean
using DataFrames: DataFrame

struct Tablefake
    rows
end

struct Queryfake 
    cleanmap
end

function evaluate_accuracy_external(dirty_data, clean_data, out_filename, verbose=false)
    table = CSV.File("$(out_filename)") |> DataFrame
    cleanmap = Dict()
    for (i, col) in enumerate(names(table))
        cleanmap[Symbol(col)] = i
    end
    rows = []
    for row in eachrow(table)
        formatted_row = Dict()
        for i in 1:length(names(table))
            formatted_row[i] = row[names(table)[i]]
        end
        push!(rows, formatted_row)
    end
    fake_table = Tablefake(rows)
    fake_query = Queryfake(cleanmap)
    return evaluate_accuracy(dirty_data, clean_data, fake_table, fake_query; verbose=verbose)
end

function get_table_from_trace(trace, query, clean_table, dirty_table)
    rows = trace.rows 
    cleanmap = query.cleanmap 

    extracted_rows = []
    num_rows = size(clean_table, 1)
    for row_index in 1:num_rows 
        extracted_row = []
        for col in names(clean_table)
            # println(col)
            col_sym = Symbol(col)
            # println(col_sym)
            if col_sym in keys(cleanmap)
                # println("in cleanmap!")
                col_index = cleanmap[col_sym]
                val = rows[row_index][col_index]
            else
                # println("not in cleanmap!")
                val = dirty_table[row_index, col]
            end
            push!(extracted_row, val)
        end
        push!(extracted_rows, extracted_row)
    end 
    matrix = mapreduce(permutedims, vcat, extracted_rows)
    df = DataFrame(matrix, names(clean_table))
    return df
end