"""
生成城市列表索引文件
"""
import os
import json

calendar_dir = os.path.join('resources', 'output', 'calendar', '2013')
output_file = os.path.join(calendar_dir, '_cities.json')

cities = []
for f in sorted(os.listdir(calendar_dir)):
    if f.endswith('.json') and not f.startswith('_'):
        city_name = f.replace('.json', '')
        cities.append(city_name)

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(cities, f, ensure_ascii=False, indent=2)

print(f"生成城市索引: {output_file}")
print(f"共 {len(cities)} 个城市")
