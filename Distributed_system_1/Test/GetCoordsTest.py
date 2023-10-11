import sys, json

file_name, id = sys.argv[1:]
id = int(id)

with open(file_name, encoding='utf-8') as file:
    data = json.load(file)

print(data['features'][id]['geometry']['coordinates'])
