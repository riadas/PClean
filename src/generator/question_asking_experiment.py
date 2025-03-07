import anthropic
import subprocess 
import os
import sys

client = anthropic.Anthropic(
    # defaults to os.environ.get("ANTHROPIC_API_KEY")
)

num_alternatives = int(sys.argv[1]) - 1
num_pclean_repeats = 3
run_id = int(sys.argv[2])

# if subset_size == 0:
#     subset_size = "full"

subset_sizes = [0, 100, 200, 300]

benchmark_name = "flights"
test_name = f"question_{benchmark_name}_num_samples_{num_alternatives + 1}_run_id_{run_id}"

schema_json_prompt = ""
error_json_with_extra_info_prompt = "" # after this, generate baseline PClean program
identify_line_to_modify_prompt = "" 
generate_question_for_clarifying_extra_info_prompt = ""
generate_sql_for_question_prompt = "" # after this, run the actual SQL to get a result
sql_query_result_prompt = "" 
improve_pclean_program_prompt = "" # after this, run the modified PClean program and output the accuracy


schema_json_prompt = """I've copied and pasted an excerpt of a dataset at the end of this message.
Could you also output the database schema in a JSON file called "table.json," with the following specification:
`tables.json` should contain the following information:
* `db_id`: database id
* `table_names`: cleaned and normalized table names for each table in the schema.
* `column_names`: original column names stored in the database. Each column looks like: `[0, "id"]`, where `0` is the index of a table name in `table_names`, and `"id"` is the column name.
* `column_types`: list containing data type of each column, in the order of the column names described above (possible types are integer, text, and time (the latter suffices for any time-related format)). 
* `foreign_keys`: foreign keys in the database. `[3, 8]` means the third column in `column_name` is a foreign key reference to the eighth column.
* `primary_keys`: list of primary keys in the database. Each number is the index of `column_names`.

Dataset Excerpt:
tuple_id,src,flight,sched_dep_time,act_dep_time,sched_arr_time,act_arr_time
1,aa,AA-3859-IAH-ORD,7:10 a.m.,7:16 a.m.,9:40 a.m.,9:32 a.m.
2,aa,AA-1733-ORD-PHX,7:45 p.m.,7:58 p.m.,10:30 p.m.,
3,aa,AA-1640-MIA-MCO,6:30 p.m.,,7:25 p.m.,
4,aa,AA-518-MIA-JFK,6:40 a.m.,6:54 a.m.,9:25 a.m.,9:28 a.m.
5,aa,AA-3756-ORD-SLC,12:15 p.m.,12:41 p.m.,2:45 p.m.,2:50 p.m.
6,aa,AA-204-LAX-MCO,11:25 p.m.,,12/02/2011 6:55 a.m.,
7,aa,AA-3468-CVG-MIA,7:00 a.m.,7:25 a.m.,9:55 a.m.,9:45 a.m.
8,aa,AA-484-DFW-MIA,4:15 p.m.,4:29 p.m.,7:55 p.m.,7:39 p.m.
9,aa,AA-446-DFW-PHL,11:50 a.m.,12:12 p.m.,3:50 p.m.,4:09 p.m.
10,aa,AA-466-IAH-MIA,6:00 a.m.,6:08 a.m.,9:20 a.m.,9:05 a.m.
11,aa,AA-1886-BOS-MIA,10:45 a.m.,10:55 a.m.,2:20 p.m.,1:40 p.m.
12,aa,AA-2957-DFW-CVG,7:55 a.m.,8:04 a.m.,11:05 a.m.,11:01 a.m.
13,aa,AA-1664-MIA-ATL,10:15 a.m.,10:18 a.m.,12:10 p.m.,11:56 a.m.
14,aa,AA-3979-CVG-ORD,7:30 a.m.,8:04 a.m.,8:00 a.m.,8:06 a.m.
15,aa,AA-1279-DFW-PHX,1:00 p.m.,2:04 p.m.,2:35 p.m.,3:30 p.m.
16,aa,AA-616-DFW-DTW,9:05 a.m.,10:10 a.m.,12:35 p.m.,1:27 p.m.
17,aa,AA-4344-ORD-DTW,11:45 a.m.,12:13 p.m.,2:00 p.m.,2:10 p.m.
18,aa,AA-2525-DFW-MIA,7:25 a.m.,7:39 a.m.,11:10 a.m.,11:06 a.m.
19,aa,AA-404-MIA-MCO,6:45 a.m.,6:57 a.m.,7:45 a.m.,7:40 a.m.
20,aa,AA-271-MIA-LAX,11:35 a.m.,11:52 a.m.,2:25 p.m.,2:27 p.m.
21,aa,AA-2050-ORD-MIA,10:40 a.m.,11:05 a.m.,2:45 p.m.,2:46 p.m.
22,aa,AA-4330-CVG-ORD,3:35 p.m.,3:36 p.m.,4:00 p.m.,3:33 p.m.
23,aa,AA-1434-DFW-MCO,7:15 a.m.,7:21 a.m.,10:35 a.m.,10:36 a.m.
24,aa,AA-4307-ORD-DTW,6:45 p.m.,6:55 p.m.,9:10 p.m.,8:53 p.m.
25,aa,AA-3-JFK-LAX,12:00 p.m.,12:11 p.m.,3:15 p.m.,3:16 p.m.
26,aa,AA-3842-MSP-ORD,6:00 a.m.,6:56 a.m.,7:30 a.m.,7:51 a.m.
27,aa,AA-643-MIA-ORD,7:10 a.m.,7:30 a.m.,9:35 a.m.,9:31 a.m.
28,aa,AA-1007-MIA-PHX,4:55 p.m.,5:08 p.m.,8:05 p.m.,7:55 p.m.
29,aa,AA-1221-MCO-ORD,8:00 p.m.,8:23 p.m.,9:45 p.m.,9:53 p.m.
30,aa,AA-400-DFW-JFK,10:40 a.m.,11:01 a.m.,2:59 p.m.,3:04 p.m.
31,aa,AA-4198-ORD-CLE,10:40 a.m.,10:54 a.m.,12:55 p.m.,12:50 p.m.
32,aa,AA-1522-SFO-ORD,11:55 p.m.,,12/02/2011 5:50 a.m.,
33,aa,AA-3823-LAX-DEN,9:00 p.m.,,12/02/2011 12:15 a.m.,
34,aa,AA-2312-DFW-DTW,8:25 p.m.,,11:50 p.m.,
35,aa,AA-1165-JFK-MIA,6:59 a.m.,7:22 a.m.,10:34 a.m.,10:04 a.m.
36,aa,AA-431-MIA-SFO,8:35 a.m.,8:51 a.m.,11:50 a.m.,11:44 a.m.
37,aa,AA-649-ORD-SNA,1:30 p.m.,,3:50 p.m.,
38,aa,AA-3063-SLC-LAX,8:20 p.m.,8:39 p.m.,9:20 p.m.,
39,aa,AA-3804-PHL-ORD,2:35 p.m.,2:44 p.m.,4:05 p.m.,3:43 p.m.
40,aa,AA-1917-JFK-MCO,2:55 p.m.,3:27 p.m.,5:55 p.m.,5:43 p.m."""

error_json_with_extra_info_prompt = """Awesome! Now, I will provide a description of errors in the dataset, and I would like you to format that error description into a JSON as well. That JSON should have the following information:

*`typos`: An array of column names from the schema JSON, representing the column names that are described to likely contain typos. For example, ['Column A'].

*`unit_errors`: An array containing elements of the following form: Each element is a length 2 array of a column name and the incorrect unit scale. For example, if a column named 'column_A' is described to have some values reported in bytes and others in megabytes, the value ['column_A', 1000000] should be in the `unit_errors` array. 

*`swaps`: An array containing elements of the following form: Each element is a length 3 array of a column name and an array of column names that identify possible rows that the first column name's value is swapped with. For example, if the 'column_A' value in a row might be swapped with the 'column_A' value of other rows with the same 'column_B' value but different 'column_C' values, then ['column_A', ['column_B'], 'column_C'] should be in the `swaps` array. For now, the length of the array in the second position must be one (e.g. like ['column_B']).

&`extra`: This should be a string containing an extra information, which doesn't neatly fit into the above error types, or provides extra information about one of the three error types. The string should be an exact copy of that part of the text in the error description.

(If any of the above three error types does not exist, that field can be omitted entirely from the output JSON. Also, the column names must exactly match the column names in the original JSON you outputted for the database schema.)

Error Description: Sometimes the scheduled departure, actual departure, scheduled arrival, and scheduled departure times of the same flight vary across different sources. It should be inferred which of these is correct so all sources provide the same information! Also, some sources are more error-prone than other sources.
"""

program = """PClean.@model FlightSchedulesModel begin
    @class Src begin
        src ~ ChooseUniformly(possibilities[:src])
    end

    @class Flights begin
        flight ~ ChooseUniformly(possibilities[:flight])
        sched_dep_time ~ TimePrior(swap_possibilities["$(flight)-sched_dep_time"])
        act_dep_time ~ TimePrior(swap_possibilities["$(flight)-act_dep_time"])
        sched_arr_time ~ TimePrior(swap_possibilities["$(flight)-sched_arr_time"])
        act_arr_time ~ TimePrior(swap_possibilities["$(flight)-act_arr_time"])
    end

    @class Obs begin
        @learned error_probs::Dict{String, ProbParameter{10.0, 50.0}}
        src ~ Src
        flights ~ Flights
        error_prob_src = error_probs[src.src]
        sched_dep_time ~ MaybeSwap(flights.sched_dep_time, swap_possibilities["$(flights.flight)-sched_dep_time"], error_prob_src)
        act_dep_time ~ MaybeSwap(flights.act_dep_time, swap_possibilities["$(flights.flight)-act_dep_time"], error_prob_src)
        sched_arr_time ~ MaybeSwap(flights.sched_arr_time, swap_possibilities["$(flights.flight)-sched_arr_time"], error_prob_src)
        act_arr_time ~ MaybeSwap(flights.act_arr_time, swap_possibilities["$(flights.flight)-act_arr_time"], error_prob_src)
    end
end

query = @query FlightSchedulesModel.Obs [
    src src.src
    flight flights.flight
    sched_dep_time flights.sched_dep_time sched_dep_time
    act_dep_time flights.act_dep_time act_dep_time
    sched_arr_time flights.sched_arr_time sched_arr_time
    act_arr_time flights.act_arr_time act_arr_time
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
"""

identify_line_to_modify_prompt = """Awesome! Below is a basic PClean program encoding all of the information in the schema JSON and error JSON that you outputted, except for the "extra" information in the error JSON. This program describes a data/error model for the dataset that can be used to clean the dataset.

{program}

Your next task is to figure out how to update this program with the "extra" information, which is so far unused (only consider the "extra" information in this part, not information in other fields of the JSON). We'll do this in a few steps. First, can you determine a line of the PClean program that should be updated, based on this extra information? You must pick just one best line, and you cannot assume other lines will be later modified, too. Copy that line in your answer. Note that you must pick a line that doesn't have the "@learned" at the start."""

schema_json_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": schema_json_prompt,
                }
            ]
        }

error_json_with_extra_info_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": error_json_with_extra_info_prompt,
                }
            ]
        }

# BEGIN MESSAGE CONSTRUCTION

print("PROMPT 1: ask for schema json")
messages = [schema_json_message]
# message = client.messages.create(
#     model="claude-3-5-sonnet-20241022",
#     max_tokens=8192,
#     temperature=0,
#     messages=messages,
# )
print("RESPONSE 1: schema json")
# print(message.content[0].text)
# with open(f"question_asking_experiment/inputs/test1_{test_name}.txt", "w+") as f:
#     print(test_name)
#     f.write(message.content[0].text)

text = ""
with open(f"question_asking_experiment/inputs/test1_{test_name}.txt", "r") as f:
    text = f.read()
print(text)
response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": text
                }
            ]
        }
messages.append(response)

print("PROMPT 2: ask for error json")
messages.append(error_json_with_extra_info_message)
# message = client.messages.create(
#     model="claude-3-5-sonnet-20241022",
#     max_tokens=8192,
#     temperature=0,
#     messages=messages,
# )

print("RESPONSE 2: error json")
# print(message.content[0].text)
# with open(f"question_asking_experiment/inputs/test2_{test_name}.txt", "w+") as f:
#     f.write(message.content[0].text)

with open(f"question_asking_experiment/inputs/test2_{test_name}.txt", "r") as f:
    text = f.read()

response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": text
                }
            ]
        }
messages.append(response)

# construct base PClean program by calling Julia generate_program
if not os.path.isdir(f'question_asking_experiment/outputs/{test_name}'):
    os.mkdir(f'question_asking_experiment/outputs/{test_name}')

command = f"gtimeout 120s julia --project=. src/generator/generate_program_wrapper.jl {test_name} {benchmark_name}"
os.system(command)

with open(f'question_asking_experiment/outputs/{test_name}/output.jl', 'r') as f:
    program = f.read()

identify_line_to_modify_prompt = identify_line_to_modify_prompt.replace("{program}", program)

identify_line_to_modify_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": identify_line_to_modify_prompt,
                }
            ]
        }

print("PROMPT 3: identify line to modify")
print(identify_line_to_modify_prompt)
messages.append(identify_line_to_modify_message)
# import ipdb 
# ipdb.set_trace()
message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=8192,
    temperature=0,
    messages=messages,
)

print("RESPONSE 3: line to modify")
print(message.content[0].text)

response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": message.content[0].text
                }
            ]
        }
messages.append(response)

# extract line to modify from response
line = list(filter(lambda y: "`" in y, list(filter(lambda x: "```" in x, message.content[0].text.split("\n\n")))[-1].split("``")))[0].replace("\n","").replace("julia", "").replace("`", "")
print("extracted line:")
print(line)
print("PROMPT 4: identify question in natural language")
generate_question_for_clarifying_extra_info_prompt = """Awesome! Now, are there any questions you'd ideally ask of the dataset, in order to clarify the "extra" information further, so it is more specific / easier to update the program with? These questions must be answerable by looking at more of the full dataset itself (i.e. not just an excerpt) -- answering them cannot require looking up other background information beyond the dataset. In other words, your questions must be translatable into single, simple SQL queries. Note that simple data exploration/access questions are okay -- no need to ask just statistical questions that requiring counting column values, for example."""
generate_question_for_clarifying_extra_info_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": generate_question_for_clarifying_extra_info_prompt,
                }
            ]
        }
messages.append(generate_question_for_clarifying_extra_info_message)
message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=8192,
    temperature=0,
    messages=messages,
)

print("RESPONSE 4: identify question in natural language")
print(message.content[0].text)

response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": message.content[0].text
                }
            ]
        }
messages.append(response)

print("PROMPT 5: translate question to a SQL query")
generate_sql_for_question_prompt = "Awesome. Let's start with only the first question. Could you translate just this question into a SQL query? If so, please output the corresponding SQL query. If multiple SQL queries are needed, just output the first SQL query. I'll then run the SQL query on the full dataset, and provide the answer to you." # after this, run the actual SQL to get a result
generate_sql_for_question_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": generate_sql_for_question_prompt,
                }
            ]
        }
messages.append(generate_sql_for_question_message)
message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=8192,
    temperature=0,
    messages=messages,
)

print("RESPONSE 5: translate question to a SQL query")
print(message.content[0].text)

response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": message.content[0].text
                }
            ]
        }
messages.append(response)

# run the SQL query
sql_query = message.content[0].text.split("\n\n")[1].replace("```", "").replace("sql", "").replace("\n", "")

shell_script = f"""#!/bin/bash
sqlite3 datasets/{benchmark_name}.db <<EOF
.headers off
.mode csv
.output test_question_sql.csv
{sql_query}
.output stdout
EOF"""

with open("test_question.sh", "w+") as f:
    f.write(shell_script)

os.system("bash test_question.sh")

with open("test_question_sql.csv", "r") as f:
    sql_response = f.read()

# # TODO: figure out how to run a SQL query
# # TODO: format answer to SQL query
print("PROMPT 6: update extra information based on SQL query")
sql_query_result_prompt = f"""Awesome! Below is the answer to this query:\n{sql_response}\nGiven this answer, can you make an informed guess that makes the "extra" information more specific?"""
sql_query_result_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": sql_query_result_prompt,
                }
            ]
        }
messages.append(sql_query_result_message)
message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=8192,
    temperature=0,
    messages=messages,
)

print("RESPONSE 6: update extra information based on SQL query")
print(message.content[0].text)

response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": message.content[0].text
                }
            ]
        }
messages.append(response)

# # TODO: figure out the exact form of this prompt
# print("PROMPT 7: improve selected line of PClean program according to prompt")
improve_pclean_program_prompt = f"""Awesome! Now, can you edit the previously selected line of the program ({line}) so that it reflects this updated extra information? The underlying language that the PClean domain-specific language is written in is Julia, so keep that in mind as you update the selected line of the program. Lastly, try to make this modification in a way so that it
1. doesn't modify any learned data structures in the program, e.g. does NOT (!!!) add new keys to a dictionary, and similarly does not assume the existence of dictionary keys that haven't been explicitly referenced,
2. preserves as much of the original line as possible, 
3. must change the behavior of the line in Julia -- DO NOT MAKE ANY ADDITIONAL ASSUMPTIONS ABOUT PCLEAN'S OPERATION,
4. doesn't multiply by 1.0 or constants greater than 1.0, and
5. uses ".current_value" whenever multiplying a variable of type ProbParameter by a constant, and the constant should be in the argument preceding the multiplication operator (but don't use ".current_value" when the variable of type ProbParameter is not being multiplied by anything!).

Notably, you're allowed to pick random float constants when updating the line, but you cannot use any dictionary key that hasn't been explicitly referenced."""

improve_pclean_program_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": improve_pclean_program_prompt,
                }
            ]
        }

messages.append(improve_pclean_program_message)


message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=8192,
    temperature=0,
    messages=messages,
)

print("RESPONSE 7: improve selected line of PClean program according to prompt")
print(message.content[0].text)

# if subset_size != "full":
#     program = program.replace("subset_size = size(dirty_table, 1)", f"subset_size = {subset_size}")

# extract the updated line, replace it within the base PClean program, and run it
updated_line = list(filter(lambda y: len(y) > 0, list(filter(lambda x: "```" in x, message.content[0].text.split("\n\n")))[0].replace("`", "").split("\n")))[-1]
updated_program = program.replace(line, updated_line)

updated_lines = []
updated_lines.append(updated_line)

if not os.path.isdir(f'question_asking_experiment/outputs/{test_name}'):
    os.mkdir(f'question_asking_experiment/outputs/{test_name}')

with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0.jl', 'w+') as f:
    f.write(updated_program)

updated_program_command = f"gtimeout 120s julia --project=. question_asking_experiment/outputs/{test_name}/output_updated_0.jl"

accuracies = {}
for subset_size in subset_sizes:
    print(f"subset_size is {subset_size}")
    accuracies[subset_size] = []
    local_accuracies = []

    if subset_size != 0:
        text = ""
        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0.jl', 'r') as f:
            text = f.read()
        text = text.replace("subset_size = size(dirty_table, 1)", f"subset_size = {subset_size}")
        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0.jl', 'w+') as f:
            f.write(text)

    for i in range(num_pclean_repeats):
        # os.system(updated_program_command)
        print(f"repeat {i} of {num_pclean_repeats}")
        try: 
            output = subprocess.run(['gtimeout', '120s', 'julia', '--project=.', f'question_asking_experiment/outputs/{test_name}/output_updated_0.jl'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE, timeout=120).stdout.decode('utf-8')
            if not os.path.isdir(f'question_asking_experiment/logs/{test_name}'):
                os.mkdir(f'question_asking_experiment/logs/{test_name}')
            
            with open(f'question_asking_experiment/logs/{test_name}/logs_updated_0.txt', 'w+') as f:
                f.write(output)

            accuracy = float(list(filter(lambda x: len(x) > 0, output.split("\n")))[-1].split(',')[0].split(' = ')[-1])
        except:
            accuracy = 0.0
        local_accuracies.append(accuracy)

    if subset_size != 0:
        text = ""
        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0.jl', 'r') as f:
            text = f.read()
        text = text.replace(f"subset_size = {subset_size}", "subset_size = size(dirty_table, 1)")
        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0.jl', 'w+') as f:
            f.write(text)

    average_accuracy = sum(local_accuracies) / num_pclean_repeats
    accuracies[subset_size].append((f'question_asking_experiment/outputs/{test_name}/output_updated_0.jl', average_accuracy, local_accuracies))

# float variants
num_variants = 2
if "*" in updated_line:
    parts = updated_line.split(" ")
    idx = parts.index("*")
    
    arg1 = parts[idx - 1]
    arg2 = parts[idx + 1]
    
    arg = ""
    if "0." in arg1: 
        arg = arg1
    elif "0." in arg2:
        arg = arg2
    
    if arg != "":
        print("FLOAT ARG TO VARY")
        print(arg)
        for i in range(1, num_variants + 1):
            new_arg = float(arg) * pow(0.01, i) 
            variant_line = updated_line.replace(arg, str(new_arg))
            updated_program = program.replace(line, variant_line)

            with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl', 'w+') as f:
                f.write(updated_program)
            
            for subset_size in subset_sizes:
                print(f"subset_size is {subset_size}")
                if subset_size != 0:
                    text = ""
                    with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl', 'r') as f:
                        text = f.read()
                    text = text.replace("subset_size = size(dirty_table, 1)", f"subset_size = {subset_size}")
                    with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl', 'w+') as f:
                        f.write(text)

                local_accuracies = []
                for j in range(num_pclean_repeats):
                    print(f"repeat {j} of {num_pclean_repeats}")
                    # os.system(updated_program_command)
                    try:
                        output = subprocess.run(['gtimeout', '120s', 'julia', '--project=.', f'question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE, timeout=120).stdout.decode('utf-8')
                        with open(f'question_asking_experiment/logs/{test_name}/logs_updated_0_variant_{i}.txt', 'w+') as f:
                            f.write(output)

                        accuracy = float(list(filter(lambda x: len(x) > 0, output.split("\n")))[-1].split(',')[0].split(' = ')[-1])
                    except:
                        accuracy = 0.0
                    local_accuracies.append(accuracy)

                if subset_size != 0:
                    text = ""
                    with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl', 'r') as f:
                        text = f.read()
                    text = text.replace(f"subset_size = {subset_size}", "subset_size = size(dirty_table, 1)")
                    with open(f'question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl', 'w+') as f:
                        f.write(text)

                average_accuracy = sum(local_accuracies) / num_pclean_repeats
                accuracies[subset_size].append((f'question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl', average_accuracy, local_accuracies))


            # updated_program_command = f"gtimeout 120s julia --project=. question_asking_experiment/outputs/{test_name}/output_updated_0_variant_{i}.jl"
            # os.system(updated_program_command)

            updated_lines.append(variant_line)

response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": message.content[0].text
                }
            ]
        }
messages.append(response)

print("PROMPT 8: try alternative program refinements")
alternatives_prompt = """Awesome, can you think of any alternative ways of making the "extra" information more specific and updating the line in the PClean program? Please show both: another guess at a more specific version of the "extra" information, and the corresponding updated PClean line."""
print(alternatives_prompt)

alternatives_message = {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": alternatives_prompt,
                }
            ]
        }
for i in range(1, num_alternatives + 1):
    print(f"RESPONSE 8: alternative {str(i)}")
    messages.append(alternatives_message)
    message = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=8192,
        temperature=0,
        messages=messages,
    )
    print(message.content[0].text)
    response = {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": message.content[0].text
                }
            ]
        }
    messages.append(response)

    # extract the updated line, replace it within the base PClean program, and run it
    updated_line = list(filter(lambda y: len(y) > 0, list(filter(lambda x: "```" in x, message.content[0].text.split("\n\n")))[0].replace("`", "").split("\n")))[-1]
    updated_program = program.replace(line, updated_line)

    updated_lines.append(updated_line)

    with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl', 'w+') as f:
        f.write(updated_program)

    for subset_size in subset_sizes:
        print(f"subset_size is {subset_size}")
        if subset_size != 0:
            text = ""
            with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl', 'r') as f:
                text = f.read()
            text = text.replace("subset_size = size(dirty_table, 1)", f"subset_size = {subset_size}")
            with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl', 'w+') as f:
                f.write(text)

        local_accuracies = []
        for j in range(num_pclean_repeats):
            print(f"repeat {j} of {num_pclean_repeats}")
            # os.system(updated_program_command)
            try:
                output = subprocess.run(['gtimeout', '120s', 'julia', '--project=.', f'question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE, timeout=120).stdout.decode('utf-8')
                with open(f'question_asking_experiment/logs/{test_name}/logs_updated_{i}.txt', 'w+') as f:
                    f.write(output)

                accuracy = float(list(filter(lambda x: len(x) > 0, output.split("\n")))[-1].split(',')[0].split(' = ')[-1])
            except:
                accuracy = 0.0
            local_accuracies.append(accuracy)

        if subset_size != 0:
            text = ""
            with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl', 'r') as f:
                text = f.read()
            text = text.replace(f"subset_size = {subset_size}", "subset_size = size(dirty_table, 1)")
            with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl', 'w+') as f:
                f.write(text)

        average_accuracy = sum(local_accuracies) / num_pclean_repeats
        accuracies[subset_size].append((f'question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl', average_accuracy, local_accuracies))


    # updated_program_command = f"gtimeout 120s julia --project=. question_asking_experiment/outputs/{test_name}/output_updated_{i}.jl"
    # os.system(updated_program_command)

    if "*" in updated_line:
        parts = updated_line.split(" ")
        idx = parts.index("*")
        
        arg1 = parts[idx - 1]
        arg2 = parts[idx + 1]
        
        arg = ""
        if "0." in arg1: 
            arg = arg1
        elif "0." in arg2:
            arg = arg2
        
        if arg != "":
            print("FLOAT ARG TO VARY")
            print(arg)
            for j in range(1, num_variants + 1):
                new_arg = float(arg) * pow(0.01, j) 
                variant_line = updated_line.replace(arg, str(new_arg))
                updated_program = program.replace(line, variant_line)

                with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl', 'w+') as f:
                    f.write(updated_program)

                for subset_size in subset_sizes: 
                    print(f"subset_size is {subset_size}")
                    if subset_size != 0:
                        text = ""
                        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl', 'r') as f:
                            text = f.read()
                        text = text.replace("subset_size = size(dirty_table, 1)", f"subset_size = {subset_size}")
                        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl', 'w+') as f:
                            f.write(text)
                                        
                    local_accuracies = []
                    for k in range(num_pclean_repeats):
                        print(f"repeat {k} of {num_pclean_repeats}")
                        # os.system(updated_program_command)
                        try:
                            output = subprocess.run(['gtimeout', '120s', 'julia', '--project=.', f'question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE, timeout=120).stdout.decode('utf-8')
                            with open(f'question_asking_experiment/logs/{test_name}/logs_updated_{i}_variant_{j}.txt', 'w+') as f:
                                f.write(output)

                            accuracy = float(list(filter(lambda x: len(x) > 0, output.split("\n")))[-1].split(',')[0].split(' = ')[-1])
                        except:
                            accuracy = 0.0
                        local_accuracies.append(accuracy)

                    if subset_size != 0:
                        text = ""
                        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl', 'r') as f:
                            text = f.read()
                        text = text.replace(f"subset_size = {subset_size}", "subset_size = size(dirty_table, 1)")
                        with open(f'question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl', 'w+') as f:
                            f.write(text)

                    average_accuracy = sum(local_accuracies) / num_pclean_repeats
                    accuracies[subset_size].append((f'question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl', average_accuracy, local_accuracies))

                # updated_program_command = f"gtimeout 120s julia --project=. question_asking_experiment/outputs/{test_name}/output_updated_{i}_variant_{j}.jl"
                # os.system(updated_program_command)

                updated_lines.append(variant_line)

for subset_size in subset_sizes:        
    with open(f'question_asking_experiment/results/{test_name}_subset_{subset_size}_results.txt', 'w+') as f:
        f.write('\n'.join(list(map(lambda x: str(x), accuracies[subset_size]))))

print("updated_lines")
print(updated_lines)

# if subset_size != "full":
#     program = program.replace(f"subset_size = {subset_size}", "subset_size = size(dirty_table, 1)")

# message = client.messages.create(
#     model="claude-3-5-sonnet-20241022",
#     max_tokens=8192,
#     temperature=0,
#     messages=[
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "I've copied and pasted an excerpt of a dataset at the end of this message.\nCould you also output the database schema in a JSON file called \"table.json,\" with the following specification:\n`tables.json` should contain the following information:\n* `db_id`: database id\n* `table_names`: cleaned and normalized table names for each table in the schema.\n* `column_names`: original column names stored in the database. Each column looks like: `[0, \"id\"]`, where `0` is the index of a table name in `table_names`, and `\"id\"` is the column name.\n* `column_types`: list containing data type of each column, in the order of the column names described above. \n* `foreign_keys`: foreign keys in the database. `[3, 8]` means the third column in `column_name` is a foreign key reference to the eighth column.\n* `primary_keys`: list of primary keys in the database. Each number is the index of `column_names`.\n\nDataset Excerpt:\ntuple_id,src,flight,sched_dep_time,act_dep_time,sched_arr_time,act_arr_time\n1,aa,AA-3859-IAH-ORD,7:10 a.m.,7:16 a.m.,9:40 a.m.,9:32 a.m.\n2,aa,AA-1733-ORD-PHX,7:45 p.m.,7:58 p.m.,10:30 p.m.,\n3,aa,AA-1640-MIA-MCO,6:30 p.m.,,7:25 p.m.,\n4,aa,AA-518-MIA-JFK,6:40 a.m.,6:54 a.m.,9:25 a.m.,9:28 a.m.\n5,aa,AA-3756-ORD-SLC,12:15 p.m.,12:41 p.m.,2:45 p.m.,2:50 p.m.\n6,aa,AA-204-LAX-MCO,11:25 p.m.,,12/02/2011 6:55 a.m.,\n7,aa,AA-3468-CVG-MIA,7:00 a.m.,7:25 a.m.,9:55 a.m.,9:45 a.m.\n8,aa,AA-484-DFW-MIA,4:15 p.m.,4:29 p.m.,7:55 p.m.,7:39 p.m.\n9,aa,AA-446-DFW-PHL,11:50 a.m.,12:12 p.m.,3:50 p.m.,4:09 p.m.\n10,aa,AA-466-IAH-MIA,6:00 a.m.,6:08 a.m.,9:20 a.m.,9:05 a.m.\n11,aa,AA-1886-BOS-MIA,10:45 a.m.,10:55 a.m.,2:20 p.m.,1:40 p.m.\n12,aa,AA-2957-DFW-CVG,7:55 a.m.,8:04 a.m.,11:05 a.m.,11:01 a.m.\n13,aa,AA-1664-MIA-ATL,10:15 a.m.,10:18 a.m.,12:10 p.m.,11:56 a.m.\n14,aa,AA-3979-CVG-ORD,7:30 a.m.,8:04 a.m.,8:00 a.m.,8:06 a.m.\n15,aa,AA-1279-DFW-PHX,1:00 p.m.,2:04 p.m.,2:35 p.m.,3:30 p.m.\n16,aa,AA-616-DFW-DTW,9:05 a.m.,10:10 a.m.,12:35 p.m.,1:27 p.m.\n17,aa,AA-4344-ORD-DTW,11:45 a.m.,12:13 p.m.,2:00 p.m.,2:10 p.m.\n18,aa,AA-2525-DFW-MIA,7:25 a.m.,7:39 a.m.,11:10 a.m.,11:06 a.m.\n19,aa,AA-404-MIA-MCO,6:45 a.m.,6:57 a.m.,7:45 a.m.,7:40 a.m.\n20,aa,AA-271-MIA-LAX,11:35 a.m.,11:52 a.m.,2:25 p.m.,2:27 p.m.\n21,aa,AA-2050-ORD-MIA,10:40 a.m.,11:05 a.m.,2:45 p.m.,2:46 p.m.\n22,aa,AA-4330-CVG-ORD,3:35 p.m.,3:36 p.m.,4:00 p.m.,3:33 p.m.\n23,aa,AA-1434-DFW-MCO,7:15 a.m.,7:21 a.m.,10:35 a.m.,10:36 a.m.\n24,aa,AA-4307-ORD-DTW,6:45 p.m.,6:55 p.m.,9:10 p.m.,8:53 p.m.\n25,aa,AA-3-JFK-LAX,12:00 p.m.,12:11 p.m.,3:15 p.m.,3:16 p.m.\n26,aa,AA-3842-MSP-ORD,6:00 a.m.,6:56 a.m.,7:30 a.m.,7:51 a.m.\n27,aa,AA-643-MIA-ORD,7:10 a.m.,7:30 a.m.,9:35 a.m.,9:31 a.m.\n28,aa,AA-1007-MIA-PHX,4:55 p.m.,5:08 p.m.,8:05 p.m.,7:55 p.m.\n29,aa,AA-1221-MCO-ORD,8:00 p.m.,8:23 p.m.,9:45 p.m.,9:53 p.m.\n30,aa,AA-400-DFW-JFK,10:40 a.m.,11:01 a.m.,2:59 p.m.,3:04 p.m.\n31,aa,AA-4198-ORD-CLE,10:40 a.m.,10:54 a.m.,12:55 p.m.,12:50 p.m.\n32,aa,AA-1522-SFO-ORD,11:55 p.m.,,12/02/2011 5:50 a.m.,\n33,aa,AA-3823-LAX-DEN,9:00 p.m.,,12/02/2011 12:15 a.m.,\n34,aa,AA-2312-DFW-DTW,8:25 p.m.,,11:50 p.m.,\n35,aa,AA-1165-JFK-MIA,6:59 a.m.,7:22 a.m.,10:34 a.m.,10:04 a.m.\n36,aa,AA-431-MIA-SFO,8:35 a.m.,8:51 a.m.,11:50 a.m.,11:44 a.m.\n37,aa,AA-649-ORD-SNA,1:30 p.m.,,3:50 p.m.,\n38,aa,AA-3063-SLC-LAX,8:20 p.m.,8:39 p.m.,9:20 p.m.,\n39,aa,AA-3804-PHL-ORD,2:35 p.m.,2:44 p.m.,4:05 p.m.,3:43 p.m.\n40,aa,AA-1917-JFK-MCO,2:55 p.m.,3:27 p.m.,5:55 p.m.,5:43 p.m."
#                 }
#             ]
#         },
#         {
#             "role": "assistant",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Based on the dataset excerpt, I'll create a `table.json` file with the schema information. The data appears to be from a single table about flight information.\n\n```json\n{\n  \"db_id\": \"flight_schedule\",\n  \"table_names\": [\"flights\"],\n  \"column_names\": [\n    [0, \"tuple_id\"],\n    [0, \"src\"],\n    [0, \"flight\"],\n    [0, \"sched_dep_time\"],\n    [0, \"act_dep_time\"],\n    [0, \"sched_arr_time\"],\n    [0, \"act_arr_time\"]\n  ],\n  \"column_types\": [\n    \"integer\",\n    \"text\",\n    \"text\",\n    \"text\",\n    \"text\",\n    \"text\",\n    \"text\"\n  ],\n  \"foreign_keys\": [],\n  \"primary_keys\": [0]\n}\n```\n\nThis schema represents:\n- A single table named \"flights\"\n- Seven columns with their respective data types\n- tuple_id as the primary key\n- No foreign keys since this is a single table\n- All time-related fields are stored as text due to the various time formats in the data\n\nThe schema captures the structure of the flight schedule data, where each row represents a flight with its scheduled and actual departure and arrival times."
#                 }
#             ]
#         },
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Awesome! Now, I will provide a description of errors in the dataset, and I would like you to format that error description into a JSON as well. That JSON should have the following information:\n\n*`typos`: An array of column names from the schema JSON, representing the column names that are described to likely contain typos. For example, ['Column A'].\n\n*`unit_errors`: An array containing elements of the following form: Each element is a length 2 array of a column name and the incorrect unit scale. For example, if a column named 'column_A' is described to have some values reported in bytes and others in megabytes, the value ['column_A', 1000000] should be in the `unit_errors` array. \n\n*`swaps`: An array containing elements of the following form: Each element is a length 3 array of a column name and an array of column names that identify possible rows that the first column name's value is swapped with. For example, if the 'column_A' value in a row might be swapped with the 'column_A' value of other rows with the same 'column_B' value but different 'column_C' values, then ['column_A', ['column_B'], 'column_C'] should be in the `swaps` array.\n\n&`extra`: This should be a string containing an extra information, which doesn't neatly fit into the above error types, or provides extra information about one of the three error types. The string should be an exact copy of that part of the text in the error description.\n\n(If any of the above three error types does not exist, that field can be omitted entirely from the output JSON. Also, the column names must exactly match the column names in the original JSON you outputted for the database schema.)\n\nError Description: Sometimes the scheduled and actual arrival and departure times of a given flight are different across different sources. It should be inferred which of these is correct so all sources provide the same information! Also, some sources are more error-prone than other sources.\n"
#                 }
#             ]
#         },
#         {
#             "role": "assistant",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Based on the error description provided, I'll create a JSON that captures the error types according to the specified format:\n\n```json\n{\n  \"swaps\": [\n    [\"sched_dep_time\", [\"flight\"], \"src\"],\n    [\"act_dep_time\", [\"flight\"], \"src\"],\n    [\"sched_arr_time\", [\"flight\"], \"src\"],\n    [\"act_arr_time\", [\"flight\"], \"src\"]\n  ],\n  \"extra\": \"some sources are more error-prone than other sources\"\n}\n```\n\nExplanation:\n- The description indicates that time values might be swapped/different across different sources for the same flight, so all four time-related columns are included in the `swaps` array. For each time column, we're indicating that values might be swapped for the same flight number (`[\"flight\"]`) but different sources (`\"src\"`).\n- The additional information about some sources being more error-prone is included in the `extra` field.\n- There are no typos or unit errors mentioned in the description, so those fields are omitted."
#                 }
#             ]
#         },
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Awesome! Below is a basic PClean program encoding all of the information in the schema JSON and error JSON that you outputted, except for the \"extra\" information in the error JSON. This program describes a data/error model for the dataset that can be used to clean the dataset.\n\nPClean.@model FlightSchedulesModel begin\n    @class Src begin\n        src ~ ChooseUniformly(possibilities[:src])\n    end\n\n    @class Flights begin\n        flight ~ ChooseUniformly(possibilities[:flight])\n        sched_dep_time ~ TimePrior(swap_possibilities[\"$(flight)-sched_dep_time\"])\n        act_dep_time ~ TimePrior(swap_possibilities[\"$(flight)-act_dep_time\"])\n        sched_arr_time ~ TimePrior(swap_possibilities[\"$(flight)-sched_arr_time\"])\n        act_arr_time ~ TimePrior(swap_possibilities[\"$(flight)-act_arr_time\"])\n    end\n\n    @class Obs begin\n        @learned error_probs::Dict{String, ProbParameter{10.0, 50.0}}\n        src ~ Src\n        flights ~ Flights\n        error_prob_src = error_probs[src.src]\n        sched_dep_time ~ MaybeSwap(flights.sched_dep_time, swap_possibilities[\"$(flights.flight)-sched_dep_time\"], error_prob_src)\n        act_dep_time ~ MaybeSwap(flights.act_dep_time, swap_possibilities[\"$(flights.flight)-act_dep_time\"], error_prob_src)\n        sched_arr_time ~ MaybeSwap(flights.sched_arr_time, swap_possibilities[\"$(flights.flight)-sched_arr_time\"], error_prob_src)\n        act_arr_time ~ MaybeSwap(flights.act_arr_time, swap_possibilities[\"$(flights.flight)-act_arr_time\"], error_prob_src)\n    end\nend\n\nquery = @query FlightSchedulesModel.Obs [\n    src src.src\n    flight flights.flight\n    sched_dep_time flights.sched_dep_time sched_dep_time\n    act_dep_time flights.act_dep_time act_dep_time\n    sched_arr_time flights.sched_arr_time sched_arr_time\n    act_arr_time flights.act_arr_time act_arr_time\n]\n\n\nobservations = [ObservedDataset(query, dirty_table)]\nconfig = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)\n@time begin \n    tr = initialize_trace(observations, config);\n    run_inference!(tr, config)\nend\n\nprintln(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))\n\nYour next task is to figure out how to update this program with the \"extra\" information, which is so far unused. We'll do this in a few steps. First, can you determine a line of the PClean program that should be updated, based on this extra information? Copy that line in your answer. Note that you must pick a line that doesn't have the \"@learned\" at the start."
#                 }
#             ]
#         },
#         {
#             "role": "assistant",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Looking at the extra information \"some sources are more error-prone than other sources\" and examining the PClean program, the line that should be updated is:\n\n```\nerror_prob_src = error_probs[src.src]\n```\n\nThis line is currently assigning error probabilities based on the source, but it's doing so in a way that treats all sources equally. Since the extra information tells us that some sources are more error-prone than others, this line should be modified to better reflect this varying reliability across sources."
#                 }
#             ]
#         },
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Awesome! Now, are there any questions you'd ideally ask of the dataset, in order to clarify the \"extra\" information further, so it is more specific / easier to update the program with? "
#                 }
#             ]
#         },
#         {
#             "role": "assistant",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Yes, I would ask these questions about the dataset:\n\n1. Which sources (values in the \"src\" column) appear in the dataset? In the excerpt I only see \"aa\" but are there others?\n\n2. Is there any prior knowledge or documentation about which specific sources are known to be more or less reliable?\n\n3. Is there any way to quantify or rank the relative error rates of different sources? For example, do we know that source X has twice the error rate of source Y?\n\n4. Are there any patterns in the types of errors each source tends to make? For example, does one source tend to have more errors in departure times versus arrival times?\n\n5. Is there any temporal pattern to source reliability? (For example, does a source become more or less reliable over time?)\n\nThese questions would help determine how to best modify the error probability assignment to reflect different error rates across sources."
#                 }
#             ]
#         },
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Awesome. Let's start with the first question. Could you translate this question into a SQL query? If so, please output the corresponding SQL query. I'll then run the SQL query on the full dataset, and provide the answer to you."
#                 }
#             ]
#         },
#         {
#             "role": "assistant",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Yes, to find all unique sources in the dataset, I would use this SQL query:\n\n```sql\nSELECT DISTINCT src \nFROM flights \nORDER BY src;\n```\n\nThis will return all unique values from the \"src\" column in alphabetical order."
#                 }
#             ]
#         },
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Awesome! Below is the answer to this query:\n\naa\nhelloflight\nboston\nweather\nairtravelcenter\nflightview\nflightstats\npanynj\nflightexplorer\nmia\niad\ndfw\nmco\nflights\nflightarrival\nflightwise\ntravelocity\nfoxbusiness\nusatoday\nmyrateplan\norbitz\nphl\nCO\nifly\nsfo\nflytecomm\nmytripandmore\nord\nwunderground\nua\nflylouisville\nquicktrip\nallegiantair\nbusinesstravellogue\nden\ngofox\nflightaware\nworld-flight-tracker\n\nGiven this answer, can you make an informed guess that makes the \"extra\" information more specific?"
#                 }
#             ]
#         },
#         {
#             "role": "assistant",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Yes! Looking at these sources, I notice there's a mix of:\n- Airlines (aa, ua, CO, allegiantair)\n- Airports (mia, iad, dfw, mco, phl, sfo, ord, den)\n- Flight tracking services (flightview, flightstats, flightexplorer, flightaware, flytecomm)\n- Travel booking sites (travelocity, orbitz, quicktrip)\n- News/weather sites (foxbusiness, usatoday, wunderground)\n- Generic/miscellaneous sources (helloflight, boston, weather, flights)\n\nGiven this variety, I can make an informed guess that the \"extra\" information \"some sources are more error-prone than others\" likely means that primary sources (airlines and airports) are probably more reliable than secondary sources (tracking services, booking sites, news sites). This is because airlines and airports would have direct access to the flight data, while other sources would be getting this information second-hand or through intermediaries.\n\nThis makes the \"extra\" information more specific: \"Sources can be categorized by type (airlines, airports, tracking services, booking sites, news/weather sites), with primary sources (airlines and airports) likely being more reliable than secondary sources.\""
#                 }
#             ]
#         },
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": "Awesome! Now, can you edit the previously selected line of the program (error_prob_src = error_probs[src.src]) so that it reflects this updated extra information? The underlying language that the PClean domain-specific language is written in is Julia, so keep that in mind as you update the selected line of the program. Lastly, try to make this modification in a way so that it\n1. doesn't modify any learned data structures in the program, e.g. does NOT (!!!) add new keys to a dictionary, and similarly does not assume the existence of dictionary keys that haven't been explicitly referenced,\n2. preserves as much of the original line as possible, and \n3. doesn't perform any arithmetic (addition, subtraction, multiplication, or division) with learned parameters,\n4. must change the behavior of the line in Julia -- DO NOT MAKE ANY ADDITIONAL ASSUMPTIONS ABOUT PCLEAN'S OPERATION.\n\nNotably, you're allowed to pick random float constants when updating the line, but you cannot use any dictionary key that hasn't been explicitly referenced."
#                 }
#             ]
#         }
#     ]
# )
# print(message.content)