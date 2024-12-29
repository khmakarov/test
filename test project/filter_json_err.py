import os
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


def remove_err_objects(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        filtered_data = []
        for obj in data:
            if not any("err" in log.get("output", {}).get("display", {})
                       for log in obj.get("log", [])):
                filtered_data.append(obj)

        if not filtered_data:
            os.remove(file_path)
            print(f"已删除空文件: {file_path}")
        else:
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(filtered_data, file, ensure_ascii=False, indent=4)
            print(f"已处理文件: {file_path}")
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
            executor.submit(remove_err_objects, file_path)
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
