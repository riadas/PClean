# data from https://allisonhorst.github.io/palmerpenguins/
results_directory = "/Users/riadas/Documents/phd/classes/databases/final_project/PClean/question_asking_experiment/results"

# import matplotlib.pyplot as plt
# import numpy as np

# species = ("Flights (JSON-Based)", "Flights (Direct Synth.)", "Rents (JSON-Based)", "Rents (Direct Synth.)", "Hospital (JSON-Based)", "Hospital (Direct Synth.)")
# penguin_means = {
#     'One-Shot Generation': (0.812, 0), #, 0, 0.236, 0.491, 0.742, 0.787),
#     'Best on 100-Row Subset': (0.812, 0),#, 0, 0.317, 0.572, 0.776, 0.801),
#     'Best on 200-Row Subset': (0.812, 0),# 0, 0.318, 0.582, 0.800, 0.798),
#     'Best on 300-Row Subset': (0.812, 0),# 0, 0.317, 0.581, 0.791, 0.807),
#     'Best on Full Data': (0.842, 0)#, 0, 0.413, 0.610, 0.798, 0.811),
# }

# x = np.arange(len(species))  # the label locations
# width = 1  # the width of the bars
# multiplier = 0

# plt.figure(figsize=(11, 8))

# fig, ax = plt.subplots(layout='constrained')

# for attribute, measurement in penguin_means.items():
#     offset = 6 * multiplier
#     rects = ax.bar(x + offset, measurement, width, label=attribute)
#     ax.bar_label(rects, padding=3)
#     multiplier += 1

# # Add some text for labels, title and custom x-axis tick labels, etc.
# ax.set_ylabel('$\mathregular{F_1}$ Score')
# ax.set_xlabel('Benchmark and Generation Method')
# ax.set_xticks(x * 6 + 2, species)
# ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

# # plt.show()

# plt.tight_layout()

# data from https://allisonhorst.github.io/palmerpenguins/

# import matplotlib.pyplot as plt
# import numpy as np

# species = ("Flights", "Rents", "Hospital")
# penguin_means = {
#     'Direct LLM Cleaning': (0.121, 0, 0.075),
#     'LLM-to-PClean: Synthesis from JSONs': (0.812, 0.236, 0.742),
#     'LLM-to-PClean: Direct Synthesis': (0, 0.491, 0.787),
#     'Expert-Written PClean' : (0.891, 0.67, 0.829),
# }

# x = np.arange(len(species))  # the label locations
# width = 0.25  # the width of the bars
# multiplier = -0.5

# fig, ax = plt.subplots(layout='constrained')

# for attribute, measurement in penguin_means.items():
#     offset = width * multiplier
#     rects = ax.bar(x + offset, measurement, width, label=attribute)
#     ax.bar_label(rects, padding=3)
#     multiplier += 1

# # Add some text for labels, title and custom x-axis tick labels, etc.
# ax.set_ylabel('Length (mm)')
# ax.set_title('Penguin attributes by species')
# ax.set_xticks(x + width, species)
# ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
# # ax.set_ylim(0, 250)

# plt.show()

# import matplotlib.pyplot as plt 
# import pandas as pd 
  
# # create data 
# df = pd.DataFrame([['A', 10, 20, 10, 30], ['B', 20, 25, 15, 25], ['C', 12, 15, 19, 6], 
#                    ['D', 10, 29, 13, 19]], 
#                   columns=['Team', 'Round 1', 'Round 2', 'Round 3', 'Round 4']) 
# # view data 
# print(df) 
  
# # plot grouped bar chart 
# df.plot(x='Team', 
#         kind='bar', 
#         stacked=False, 
        # title='Grouped Bar Graph with dataframe') 


import matplotlib.pyplot as plt 
import numpy as np 
  
# create data 
x = np.arange(3) 
y1 = [0.121, 0, 0.075] 
y2 = [0.812, 0.236, 0.742] 
y3 = [0, 0.491, 0.787] 
y4 = [0.891, 0.67, 0.829]
width = 0.2

#     'Direct LLM Cleaning': (0.121, 0, 0.075),
#     'LLM-to-PClean: Synthesis from JSONs': (0.812, 0.236, 0.742),
#     'LLM-to-PClean: Direct Synthesis': (0, 0.491, 0.787),
#     'Expert-Written PClean' : (0.891, 0.67, 0.829),
  
# plot data in grouped manner of bar type 
a = plt.bar(x-0.3, y1, width, color='#ffa600') 
plt.bar_label(a, y1)

b = plt.bar(x-0.1, y2, width, color='#ef5675') 
plt.bar_label(b, y2)

c = plt.bar(x+0.1, y3, width, color='#7a5195') 
plt.bar_label(c, y3)

d = plt.bar(x+0.3, y4, width, color='#003f5c') 
plt.bar_label(d, y4)

plt.title("$\mathregular{F_1}$ Scores of One-Shot LLM-to-PClean Programs and Baselines")
plt.xticks(x, ['Flights', 'Rents', 'Hospital']) 
plt.xlabel("Benchmarks")
plt.ylabel("$\mathregular{F_1}$ Score") 
plt.legend(["Direct LLM Cleaning", "LLM-to-PClean: Synthesis from JSONs", "LLM-to-PClean: Direct Synthesis", "Expert-Written PClean"], loc='center left', bbox_to_anchor=(1, 0.5)) 
plt.tight_layout()
plt.show() 

# ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


# x = np.arange(5) * 1.4
# y1 = [0.812, 0.236, 0.491, 0.742, 0.787] 
# y2 = [0.812, 0.317, 0.572, 0.776, 0.801] 
# y3 = [0.812, 0.318, 0.582, 0.800, 0.798] 
# y4 = [0.812, 0.317, 0.581, 0.791, 0.807]
# y5 = [0.842, 0.413, 0.610, 0.798, 0.811]
# y6 = [0.891, 0.670, 0.670, 0.829, 0.829]
# width = 0.2

# #     'Direct LLM Cleaning': (0.121, 0, 0.075),
# #     'LLM-to-PClean: Synthesis from JSONs': (0.812, 0.236, 0.742),
# #     'LLM-to-PClean: Direct Synthesis': (0, 0.491, 0.787),
# #     'Expert-Written PClean' : (0.891, 0.67, 0.829),
  
# # plot data in grouped manner of bar type 
# a = plt.bar(x-0.5, y1, width, color='#ffa600') 
# # plt.bar_label(a, y1)

# b = plt.bar(x-0.3, y2, width, color='#ff6e54') 
# # plt.bar_label(b, y2)

# c = plt.bar(x-0.1, y3, width, color='#dd5182') 
# # plt.bar_label(c, y3)

# d = plt.bar(x+0.1, y4, width, color='#955196') 
# # plt.bar_label(d, y4)

# e = plt.bar(x+0.3, y5, width, color='#444e86') 
# # plt.bar_label(e, y5)

# f = plt.bar(x+0.5, y6, width, color='#003f5c') 
# # plt.bar_label(f, y6)

# plt.title("$\mathregular{F_1}$ Scores of Optimal One-Shot PClean Program Variants and Baselines")
# plt.xticks(x, ["Flights (JSON-Based)", "Rents (JSON-Based)", "Rents (Direct Synth.)", "Hospital (JSON-Based)", "Hospital (Direct Synth.)"]) 
# plt.xlabel("Benchmarks")
# plt.ylabel("$\mathregular{F_1}$ Score") 
# plt.legend(["One-Shot Baseline", "Best on Subset: 100 Rows", "Best on Subset: 200 Rows", "Best on Subset: 300 Rows", "Best on Full Data", "Expert-Written PClean"], loc='center left', bbox_to_anchor=(1, 0.5)) 
# plt.tight_layout()
# plt.show() 

plt.savefig(f'{results_directory}/one_shot_variants_real.png')