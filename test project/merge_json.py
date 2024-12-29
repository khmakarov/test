import os
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


def load_json_files(file_paths):
    all_data = []
    for file_path in file_paths:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                all_data.extend(data)
        except Exception as e:
            print(f"读取文件 {file_path} 时出错: {e}")
    return all_data


def save_json_files(data, output_directory, base_name, max_objects=200):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    total_files = (len(data) + max_objects - 1) // max_objects
    for i in range(total_files):
        part_data = data[i * max_objects:(i + 1) * max_objects]
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


def process_folder(folder_path, output_directory, max_objects=200):
    file_paths = [
        os.path.join(folder_path, file) for file in os.listdir(folder_path)
        if file.endswith('.json')
    ]
    if file_paths:
        all_data = load_json_files(file_paths)
        if all_data:
            base_name = os.path.basename(folder_path)
            save_json_files(all_data, output_directory, base_name, max_objects)


def process_directory(input_directory,
                      output_directory,
                      max_workers=8,
                      max_objects=200):
    folder_paths = [
        os.path.join(input_directory, dir)
        for dir in os.listdir(input_directory)
        if os.path.isdir(os.path.join(input_directory, dir))
    ]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_folder, folder_path, output_directory,
                            max_objects) for folder_path in folder_paths
        ]
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                future.result()
            except Exception as e:
                print(f"处理过程中出现错误: {e}")


if __name__ == "__main__":
    input_directory = r"E:\VSCPython\Amazons\dataset"
    output_directory = r"E:\VSCPython\Amazons\dataset\merge"
    max_workers = 8
    max_objects = 200
    process_directory(input_directory,
                      output_directory,
                      max_workers=max_workers,
                      max_objects=max_objects)
