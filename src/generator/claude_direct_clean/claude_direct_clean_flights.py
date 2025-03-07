import anthropic
import copy
import time

client = anthropic.Anthropic(
)

# goal: prompt information necessary to generate a PClean program

# step 1 claude: spit out schema in a JSON given natural language text + an excerpt of 
# step 1 script: convert to desired form for generating PClean program

# step 2 claude: spit out representation of generative data/error model from text
# step 2 script: 

# step 3 script: construct final PClean program given extracted data and write to file
# (step 4+: run the generated file using PClean)

with open('src/generator/claude_direct_clean/first_user_message.txt') as f:
    input_text = f.read()

messages = [{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": input_text
                    }
                ]
            }]

message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=3000,
    temperature=0,
    system="",
    messages=messages
)

print("FIRST RESPONSE")
print(message.content[0].text)
with open("src/generator/claude_direct_clean/responses/response_0.txt", "w+") as f:
    f.write(message.content[0].text)

current_assistant_response = {"role": "assistant",
                              "content": [
                                {
                                "type": "text",
                                "text": message.content[0].text
                                }
                                ]
                            }
new_prompt = {
        "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Yes, please!"
                }
            ]
}

for i in range(1, 50):
    time.sleep(300) # wait five minutes
    messages.append(current_assistant_response)
    messages.append(new_prompt)
    next_message = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=3000,
        temperature=0,
        messages=messages
    )

    print("RESPONSE " + str(i))
    print(next_message.content[0].text)
    with open("src/generator/claude_direct_clean/responses/response_" + str(i) + ".txt", "w+") as f:
        f.write(next_message.content[0].text)
    current_assistant_response = copy.deepcopy(current_assistant_response)
    current_assistant_response["content"][0]["text"] = next_message.content[0].text


# typos, units, averages, maybe swap
# typos -- only need the column name
# units -- column name, plus unit options (maybe [1, 1000]?)
# maybe swap -- column name (manually initialize with default error probabilities)