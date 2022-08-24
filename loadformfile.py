with open('resources/freqList.txt') as f:
    lines = f.read().splitlines()

i = 0
while i < len(lines):
    print(lines[i].split(";"))
    i = i + 1
