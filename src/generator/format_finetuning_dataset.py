import sys 
datasets_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean/finetuning_datasets"
filename = sys.argv[1]
# print(filename)
# print(datasets_directory + "/"+filename)
with open(datasets_directory + "/"+filename, "r+") as f:
    text = f.read()
    print("\$" in text)
    new_text = text.replace("\$", "$")
    print("\$" in new_text)
    f.seek(0)
    f.write(new_text)
    f.truncate()