import matplotlib.pyplot as plt

skill_list = open('skills', 'r')
skill_recruited = open('xi_kiu', 'r')

skills = [line.strip('\n') for line in skill_recruited if line.strip()]
# print(skills)

skill_dict = {}
for s in skills:
    if s not in skill_dict:
        skill_dict[s] = 0
    else:
        skill_dict[s] += 1
# print(skill_dict)
skill_dict = dict(sorted(skill_dict.items(), key=lambda item: item[1]))
print(skill_dict)
names = list(skill_dict.keys())
counts = list(skill_dict.values())
plt.figure(figsize=(18, 20))
plt.barh(range(len(names)), counts)
plt.yticks(range(len(names)), names)
plt.show()

skill_list.close()
skill_recruited.close()