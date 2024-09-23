import re
import json

def read_txt_file(filename):
    try:
        with open(filename, 'r') as file:
            content = file.readlines()
        
        # 创建一个列表来存储槽键
        slot_list = []
        
        for line in content:
            line = line.strip().strip(',')  # 去掉行首尾的空白和逗号
            # 使用正则表达式提取 slot 和内容
            match = re.match(r'slot-(\d+)-\{(.+?)\}', line)
            if match:
                num = int(match.group(1))  # 下标
                value = match.group(2)      # 括号里的内容
                slot_list.append((num, value))  # 以元组形式添加到列表

        return slot_list
    
    except FileNotFoundError:
        print(f"文件 {filename} 未找到。")
    except Exception as e:
        print(f"读取文件时发生错误: {e}")

def write_to_json(data, output_file):
    try:
        with open(output_file, 'w') as json_file:
            json.dump(data, json_file, indent=2)  # 写入 JSON 文件，缩进为 2
        print(f"数据已成功写入 {output_file} 文件。")
    except Exception as e:
        print(f"写入文件时发生错误: {e}")

# 读取 slots.txt 文件并拆分字符串
slot_data = read_txt_file('slots.txt')

# 写入到 JSON 文件
write_to_json(slot_data, 'slots.json')