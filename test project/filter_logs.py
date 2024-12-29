import os
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


def process_log(log):
    if len(log) <= 20:
        return None
    new_log = []
    for entry in log:
        if '0' in entry or '1' in entry:
            for key in ('0', '1'):
                if key in entry:
                    sub_entry = entry[key]
                    for field in ('memory', 'time', 'verdict', 'debug',
                                  'keep_running'):
                        if field in sub_entry:
                            del sub_entry[field]
            new_log.append(entry)
        else:
            output = entry.get('output', {})
            command = output.get('command')
            if command == 'request':
                continue
            elif command == 'finish':
                for field in ('memory', 'time', 'verdict', 'debug',
                              'keep_running'):
                    if field in entry:
                        del entry[field]
                if 'display' in output:
                    del output['display']
                new_log.append(entry)
            else:
                new_log.append(entry)
    return new_log


def process_json_object(json_object):
    if "initdata" in json_object:
        del json_object["initdata"]

    if "players" in json_object:
        players = json_object["players"]
        if not all(player.get("type") == "bot" for player in players):
            return None  # 如果两个 "type" 字段的值不都为 "bot"，则返回 None

    if "log" in json_object:
        new_log = process_log(json_object["log"])
        if new_log is None:
            return None
        json_object["log"] = new_log

    return json_object


def process_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        processed_data = []
        for obj in data:
            processed_obj = process_json_object(obj)
            if processed_obj is not None:
                processed_data.append(processed_obj)

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write('[\n')
            for i, obj in enumerate(data):
                if i > 0:
                    file.write(',\n')
                json.dump(obj, file, ensure_ascii=False)
            file.write('\n]')
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {e}")


def process_directory(directory_path, max_workers=8):
    file_paths = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.json'):
                file_paths.append(os.path.join(root, file))

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_json_file, file_path)
            for file_path in file_paths
        ]
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                future.result()
            except Exception as e:
                print(f"处理过程中出现错误: {e}")


if __name__ == "__main__":
    directory_path = r"E:\VSCPython\Amazons\dataset\merge"
    max_workers = 16
    process_directory(directory_path, max_workers=max_workers)
