# input: table_json -- need column names and column types
function generate_errors(table_json)

end

"""
{
  "swaps": [
    ["sched_dep_time", ["flight"], "src"],
    ["act_dep_time", ["flight"], "src"],
    ["sched_arr_time", ["flight"], "src"],
    ["act_arr_time", ["flight"], "src"]
  ]
}

{
    "typos": ["county"],
    "unit_errors": [["monthly_rent", 1000]]
}
"""