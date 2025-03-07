import os 
import sys
import anthropic

client = anthropic.Anthropic(
    # defaults to os.environ.get("ANTHROPIC_API_KEY")
)

test_name = sys.argv[1]
print(test_name)

with open(f'src/generator/inputs/{test_name}_schema_prompt.txt', 'r') as f:
    schema_json_prompt = f.read()

with open(f'src/generator/inputs/{test_name}_error_prompt.txt', 'r') as f:
    error_json_prompt = f.read()

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
                    "text": error_json_prompt,
                }
            ]
        }

# BEGIN MESSAGE CONSTRUCTION

print("PROMPT 1: ask for schema json")
messages = [schema_json_message]
message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=8192,
    temperature=0,
    messages=messages,
)
print("RESPONSE 1: schema json")
print(message.content[0].text)
# with open(f"src/generator/test1_{test_name}.txt", "w+") as f:
#     print(test_name)
#     f.write(message.content[0].text)

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

print("PROMPT 2: ask for error json")
messages.append(error_json_with_extra_info_message)
message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=8192,
    temperature=0,
    messages=messages,
)

print("RESPONSE 2: error json")
print(message.content[0].text)
# with open(f"src/generator/test2_{test_name}.txt", "w+") as f:
#     f.write(message.content[0].text)