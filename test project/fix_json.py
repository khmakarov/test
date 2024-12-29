import os
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


def fix_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        json_objects = []
        buffer = ''
        depth = 0

        for char in content:
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1

            buffer += char

            if depth == 0 and buffer.strip():
                try:
                    json_objects.append(json.loads(buffer))
                except json.JSONDecodeError as e:
                    print(f"JSON解码错误: {e} 在文件 {file_path}")
                    return
                buffer = ''

        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(json_objects, file, ensure_ascii=False, indent=4)

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
            executor.submit(fix_json_file, file_path)
            for file_path in file_paths
        ]
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                future.result()
            except Exception as e:
                print(f"处理过程中出现错误: {e}")


if __name__ == "__main__":
    directory_path = r"E:\VSCPython\Amazons\dataset"
    max_workers = 8
    process_directory(directory_path, max_workers=max_workers)
