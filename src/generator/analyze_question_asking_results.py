import os
import matplotlib.pyplot as plt
import numpy as np
import subprocess

home_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean"
results_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean/question_asking_experiment/results"

filenames =  os.listdir(results_directory)

# TODO: change so that for subsets, we compute the accuracy of running that program (subset-maximizing one) on the full dataset.

subset_sizes = [0, 100, 200, 300]
sample_sizes = [1, 5, 10] # [1, 2]
results = []
for subset_size in subset_sizes:
    print(f"subset_size is {subset_size}")
    selected_files = list(filter(lambda x: f"subset_{subset_size}" in x, filenames))
    print(selected_files)
    for k in sample_sizes:
        print(f"sample_size is {k}")
        subset_size_results = []
        for file in selected_files:
            with open(f"{results_directory}/{file}", "r") as f:
                text = f.read()
            all = list(map(lambda x: eval(x), text.split("\n")))[:(k * 3)]
            print(all)
            if subset_size == 0:
                highest_accuracy = max(list(filter(lambda x: x, all)), key=lambda tup : tup[1])[1] 
            else:
                highest_accuracy_tuple = max(list(filter(lambda x: x, all)), key=lambda tup : tup[1])
                if highest_accuracy_tuple[1] == 0:
                    highest_accuracy = 0.0
                else: 
                    accuracies = []
                    for i in range(3):
                        print(f"repeat {i} of 3")
                        julia_filename = highest_accuracy_tuple[0] #.replace("outputs", "old/outputs")
                        print(julia_filename)
                        output = subprocess.run(['gtimeout', '120s', 'julia', '--project=.', f'{home_directory}/{julia_filename}'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE, timeout=120).stdout.decode('utf-8')
                        accuracy = float(list(filter(lambda x: len(x) > 0, output.split("\n")))[-1].split(',')[0].split(' = ')[-1])
                        accuracies.append(accuracy)
                    highest_accuracy = np.mean(np.array(accuracies))
            subset_size_results.append(highest_accuracy)
        if subset_size == 0:
            results.append((k, 1000, sum(subset_size_results)/len(subset_size_results), subset_size_results))
        else:
            results.append((k, subset_size, sum(subset_size_results)/len(subset_size_results), subset_size_results))

results = sorted(results, key=lambda tup: (tup[0], tup[1]))
with open(f'{results_directory}/aggregate_results.txt', 'w+') as f:
    f.write('\n'.join(list(map(lambda x: str(x), results))))

with open(f'{results_directory}/aggregate_results.txt', 'r') as f:
    results = list(map(lambda l: eval(l), f.read().split("\n")))

# boxplot data
data = list(map(lambda x: x[-1], results))
for i in range(len(data)):
    data[i] = list(filter(lambda x: x != 0.0, data[i]))

fig = plt.figure(figsize = (7, 5))

# Creating axes instance
# ax = fig.add_axes([0, 0, 1, 1])
ax = fig.add_subplot(111)
labels = list(map(lambda tup: (f"k={tup[0]}").replace("1000", "all"), results))


updated_data = []
updated_labels = []

for i in [3, 7, 11]:
    updated_data.append(data[i])
    updated_labels.append(labels[i])

ax.set_xticklabels(updated_labels)
# ax.tick_params(axis='x', labelrotation=300)

plt.title("Distribution of Best $\mathregular{F_1}$ Scores over\nProgram Refinement Experiments (N=6)")
plt.xlabel("Number of Sampled Program Edits")
plt.ylabel("$\mathregular{F_1}$ Score")

left, right = ax.get_xlim()
plt.axhline(0.891, color='blue', linestyle=':', label='Expert-Written PClean')
plt.axhline(0.812, color='red', linestyle=':', label='One-Shot Generation')
plt.legend()

# Creating plot
bp = ax.boxplot(updated_data)

plt.tight_layout()

# show plot
fig.savefig(f'{results_directory}/fig2.png')
# plt.show()