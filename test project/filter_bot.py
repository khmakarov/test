import os
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


def load_bot_ids(bot_file_path):
    with open(bot_file_path, 'r', encoding='utf-8') as file:
        return set(line.strip() for line in file)


def filter_json_objects(data, bot_ids):
    filtered_data = []
    for obj in data:
        if 'players' in obj:
            players = obj['players']
            if any(player['bot'] in bot_ids for player in players):
                filtered_data.append(obj)
    return filtered_data


def process_json_file(file_path, bot_ids):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        filtered_data = filter_json_objects(data, bot_ids)
        return filtered_data

    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {e}")
        return []


def save_filtered_data(filtered_data,
                       output_directory,
                       base_name,
                       max_objects=200):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    total_files = (len(filtered_data) + max_objects - 1) // max_objects
    for i in range(total_files):
        part_data = filtered_data[i * max_objects:(i + 1) * max_objects]
        file_path = os.path.join(output_directory,
                                 f"{base_name}_part{i+1}.json")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write('[\n')
            for j, obj in enumerate(part_data):
                if j > 0:
                    file.write(',\n')
                json.dump(obj, file, ensure_ascii=False)
            file.write('\n]')
        print(f"已保存文件: {file_path}")


def process_directory(input_directory,
                      output_directory,
                      bot_file_path,
                      max_workers=8,
                      max_objects=200):
    bot_ids = load_bot_ids(bot_file_path)

    all_filtered_data = []
    file_paths = [
        os.path.join(input_directory, file)
        for file in os.listdir(input_directory) if file.endswith('.json')
    ]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_json_file, file_path, bot_ids)
            for file_path in file_paths
        ]
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                all_filtered_data.extend(future.result())
            except Exception as e:
                print(f"处理过程中出现错误: {e}")

    save_filtered_data(all_filtered_data, output_directory, 'filtered_data',
                       max_objects)


if __name__ == "__main__":
    input_directory = r"E:\VSCPython\Amazons\dataset\merge"
    output_directory = r"E:\VSCPython\Amazons\dataset\top120"
    bot_file_path = r"E:\VSCPython\Amazons\dataProcess\bot.txt"
    max_workers = 8
    max_objects = 200
    process_directory(input_directory,
                      output_directory,
                      bot_file_path,
                      max_workers=max_workers,
                      max_objects=max_objects)
