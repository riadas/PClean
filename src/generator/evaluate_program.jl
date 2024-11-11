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