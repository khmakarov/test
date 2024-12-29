import os
import json
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


def is_valid_json(obj):
    # 检查对象中是否包含乱码字符（例如 �）
    obj_str = json.dumps(obj, ensure_ascii=False)
    return not re.search(r'[^\u0000-\uD7FF\uE000-\uFFFF]', obj_str)


def compress_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        valid_data = [obj for obj in data if is_valid_json(obj)]

        if not valid_data:
            os.remove(file_path)
            print(f"已删除空文件: {file_path}")
        else:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write('[\n')
                for i, obj in enumerate(valid_data):
                    if i > 0:
                        file.write(',\n')
                    json.dump(obj, file, ensure_ascii=False)
                file.write('\n]')
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
            executor.submit(compress_json_file, file_path)
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
