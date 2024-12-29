import os
import json
import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from collections import defaultdict
from typing import Dict
from tqdm import tqdm


class Coordinates:

    def __init__(self, x, y):
        self.x = x
        self.y = y


class Action:

    def __init__(self,
                 start_x=-1,
                 start_y=-1,
                 end_x=-1,
                 end_y=-1,
                 barrier_x=-1,
                 barrier_y=-1):
        self.start_x = start_x
        self.start_y = start_y
        self.end_x = end_x
        self.end_y = end_y
        self.barrier_x = barrier_x
        self.barrier_y = barrier_y


class Board:

    def __init__(self):
        self.chessboard = [[0] * 8 for _ in range(8)]
        self.temp = 0
        self.chessboard[0][2] = 1
        self.chessboard[2][0] = 1
        self.chessboard[5][0] = 1
        self.chessboard[7][2] = 1
        self.chessboard[0][5] = -1
        self.chessboard[2][7] = -1
        self.chessboard[5][7] = -1
        self.chessboard[7][5] = -1

    @staticmethod
    def is_valid_map(x, y):
        return 0 <= x < 8 and 0 <= y < 8

    def get_piece(self, x, y):
        return self.chessboard[x][y]

    def can_do(self, nx, ny):
        return self.is_valid_map(nx, ny) and self.chessboard[nx][ny] == 0

    def move_piece(self, x, y, nx, ny):
        self.chessboard[nx][ny] = self.chessboard[x][y]
        self.chessboard[x][y] = 0

    def place_block(self, x, y):
        self.chessboard[x][y] = 2

    def clear(self, x, y):
        self.temp = self.chessboard[x][y]
        self.chessboard[x][y] = 0

    def restore(self, x, y, nx, ny, bx, by):
        if nx == -1:
            self.chessboard[x][y] = self.temp
            self.temp = 0
        else:
            if x == bx and y == by:
                self.chessboard[x][y] = self.chessboard[nx][ny]
                self.chessboard[nx][ny] = 0
            else:
                self.chessboard[x][y] = self.chessboard[nx][ny]
                self.chessboard[nx][ny] = 0
                self.chessboard[bx][by] = 0


def expand_move(chessboard, player):
    dx = [-1, -1, -1, 0, 0, 1, 1, 1]
    dy = [-1, 0, 1, -1, 1, -1, 0, 1]
    legal_moves = []
    piece = []
    count = 0
    is_finished = False

    for i in range(8):
        for j in range(8):
            if chessboard.get_piece(i, j) == player:
                piece.append(Coordinates(i, j))
                count += 1
                if count == 4:
                    is_finished = True
                    break
        if is_finished:
            break

    for p in piece:
        x, y = p.x, p.y
        for idx in range(8):
            nx, ny = x + dx[idx], y + dy[idx]
            while chessboard.can_do(nx, ny):
                chessboard.clear(x, y)
                for t_idx in range(8):
                    bx, by = nx + dx[t_idx], ny + dy[t_idx]
                    while chessboard.can_do(bx, by):
                        legal_moves.append(Action(x, y, nx, ny, bx, by))
                        bx += dx[t_idx]
                        by += dy[t_idx]
                chessboard.restore(x, y, -1, -1, -1, -1)
                nx += dx[idx]
                ny += dy[idx]

    return legal_moves


def serialize_move(move: Action) -> str:
    return f'{move.start_x},{move.start_y},{move.end_x},{move.end_y},{move.barrier_x},{move.barrier_y}'


def calculate_probabilities(frequencies: Dict[str, int],
                            probabilities: Dict[str, float]):
    total_moves = sum(frequencies.values())
    for move, count in frequencies.items():
        probabilities[move] = count / total_moves


def calculate_win_rate(games: Dict[str, int], win_games: Dict[str, int],
                       win_rate: Dict[str, Dict[str, float]]):
    for move, count in win_games.items():
        total_games = games.get(move, 0)
        if total_games > 0:
            win_rate[move] = {
                'count': count,
                'total_games': total_games,
                'win_rate': count / total_games
            }


def write_moves_to_csv(move_probabilities: Dict[str, float], filename: str):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Move", "Probability"])
        for move, probability in sorted(move_probabilities.items()):
            if probability != 0.0:
                writer.writerow([move, probability])


def write_win_rate_to_csv(win_rate: Dict[str, Dict[str, float]],
                          filename: str):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Move", "Count", "TotalGames", "WinRate"])
        for move, stats in sorted(win_rate.items()):
            writer.writerow([
                move, stats['count'], stats['total_games'], stats['win_rate']
            ])


def process_file(file_path: str):
    with open(file_path, 'r') as file:
        match_data = json.load(file)

    local_data = {
        'black_move_frequencies': defaultdict(int),
        'white_move_frequencies': defaultdict(int),
        'black_move_frequencies_opening': defaultdict(int),
        'white_move_frequencies_opening': defaultdict(int),
        'black_move_frequencies_middle': defaultdict(int),
        'white_move_frequencies_middle': defaultdict(int),
        'black_move_frequencies_end': defaultdict(int),
        'white_move_frequencies_end': defaultdict(int),
        'black_win_games': defaultdict(int),
        'white_win_games': defaultdict(int),
        'black_win_games_opening': defaultdict(int),
        'white_win_games_opening': defaultdict(int),
        'black_win_games_middle': defaultdict(int),
        'white_win_games_middle': defaultdict(int),
        'black_win_games_end': defaultdict(int),
        'white_win_games_end': defaultdict(int),
        'black_games': defaultdict(int),
        'white_games': defaultdict(int),
        'black_games_opening': defaultdict(int),
        'white_games_opening': defaultdict(int),
        'black_games_middle': defaultdict(int),
        'white_games_middle': defaultdict(int),
        'black_games_end': defaultdict(int),
        'white_games_end': defaultdict(int),
    }

    for obj in match_data:
        chessboard = Board()
        steps = len(obj["log"])
        winner = 0 if obj["scores"][0] == 2 else 1
        for i in range(steps - 1):
            response = obj["log"][i][str(i % 2)]["response"]
            move = Action(response["x0"], response["y0"], response["x1"],
                          response["y1"], response["x2"], response["y2"])

            if i < 12:
                current_frequencies = local_data[
                    'black_move_frequencies_opening'] if i % 2 == 0 else local_data[
                        'white_move_frequencies_opening']
                current_games = local_data[
                    'black_games_opening'] if i % 2 == 0 else local_data[
                        'white_games_opening']
                current_win_games = local_data[
                    'black_win_games_opening'] if winner == 0 else local_data[
                        'white_win_games_opening']
            elif i < 44:
                current_frequencies = local_data[
                    'black_move_frequencies_middle'] if i % 2 == 0 else local_data[
                        'white_move_frequencies_middle']
                current_games = local_data[
                    'black_games_middle'] if i % 2 == 0 else local_data[
                        'white_games_middle']
                current_win_games = local_data[
                    'black_win_games_middle'] if winner == 0 else local_data[
                        'white_win_games_middle']
            else:
                current_frequencies = local_data[
                    'black_move_frequencies_end'] if i % 2 == 0 else local_data[
                        'white_move_frequencies_end']
                current_games = local_data[
                    'black_games_end'] if i % 2 == 0 else local_data[
                        'white_games_end']
                current_win_games = local_data[
                    'black_win_games_end'] if winner == 0 else local_data[
                        'white_win_games_end']

            move_str = serialize_move(move)
            current_frequencies[move_str] += 1
            current_games[move_str] += 1
            if i % 2 == winner:
                current_win_games[move_str] += 1

            overall_frequencies = local_data[
                'black_move_frequencies'] if i % 2 == 0 else local_data[
                    'white_move_frequencies']
            overall_games = local_data[
                'black_games'] if i % 2 == 0 else local_data['white_games']
            overall_win_games = local_data[
                'black_win_games'] if winner == 0 else local_data[
                    'white_win_games']
            overall_frequencies[move_str] += 1
            overall_games[move_str] += 1
            if i % 2 == winner:
                overall_win_games[move_str] += 1

            chessboard.move_piece(move.start_x, move.start_y, move.end_x,
                                  move.end_y)
            chessboard.place_block(move.barrier_x, move.barrier_y)

    return local_data


def merge_dictionaries(target: Dict, source: Dict):
    for key, value in source.items():
        if key in target:
            target[key] += value
        else:
            target[key] = value


all_files_data = {
    'black_move_frequencies': defaultdict(int),
    'white_move_frequencies': defaultdict(int),
    'black_move_frequencies_opening': defaultdict(int),
    'white_move_frequencies_opening': defaultdict(int),
    'black_move_frequencies_middle': defaultdict(int),
    'white_move_frequencies_middle': defaultdict(int),
    'black_move_frequencies_end': defaultdict(int),
    'white_move_frequencies_end': defaultdict(int),
    'black_win_games': defaultdict(int),
    'white_win_games': defaultdict(int),
    'black_win_games_opening': defaultdict(int),
    'white_win_games_opening': defaultdict(int),
    'black_win_games_middle': defaultdict(int),
    'white_win_games_middle': defaultdict(int),
    'black_win_games_end': defaultdict(int),
    'white_win_games_end': defaultdict(int),
    'black_games': defaultdict(int),
    'white_games': defaultdict(int),
    'black_games_opening': defaultdict(int),
    'white_games_opening': defaultdict(int),
    'black_games_middle': defaultdict(int),
    'white_games_middle': defaultdict(int),
    'black_games_end': defaultdict(int),
    'white_games_end': defaultdict(int),
}


def process_directory():
    directory_path = r"E:\VSCPython\Amazons\dataset\merge"

    json_files = [
        os.path.join(directory_path, file)
        for file in os.listdir(directory_path) if file.endswith('.json')
    ]

    max_processes = 8

    with ProcessPoolExecutor(max_workers=max_processes) as process_executor:
        futures = {
            process_executor.submit(process_file, file): file
            for file in json_files
        }

        for future in tqdm(as_completed(futures),
                           total=len(futures),
                           desc="Processing files"):
            local_data = future.result()
            for key in all_files_data.keys():
                merge_dictionaries(all_files_data[key], local_data[key])


if __name__ == "__main__":
    process_directory()
    black_move_probabilities_opening = {}
    white_move_probabilities_opening = {}
    black_move_probabilities_middle = {}
    white_move_probabilities_middle = {}
    black_move_probabilities_end = {}
    white_move_probabilities_end = {}
    black_move_probabilities = {}
    white_move_probabilities = {}
    black_win_rate_opening = {}
    white_win_rate_opening = {}
    black_win_rate_middle = {}
    white_win_rate_middle = {}
    black_win_rate_end = {}
    white_win_rate_end = {}
    black_win_rate = {}
    white_win_rate = {}
    '''
    calculate_probabilities(all_files_data['black_move_frequencies_opening'],
                            black_move_probabilities_opening)
    calculate_probabilities(all_files_data['white_move_frequencies_opening'],
                            white_move_probabilities_opening)
    calculate_probabilities(all_files_data['black_move_frequencies_middle'],
                            black_move_probabilities_middle)
    calculate_probabilities(all_files_data['white_move_frequencies_middle'],
                            white_move_probabilities_middle)
    calculate_probabilities(all_files_data['black_move_frequencies_end'],
                            black_move_probabilities_end)
    calculate_probabilities(all_files_data['white_move_frequencies_end'],
                            white_move_probabilities_end)
    calculate_probabilities(all_files_data['black_move_frequencies'],
                            black_move_probabilities)
    calculate_probabilities(all_files_data['white_move_frequencies'],
                            white_move_probabilities)
    '''
    calculate_win_rate(all_files_data['black_games_opening'],
                       all_files_data['black_win_games_opening'],
                       black_win_rate_opening)
    calculate_win_rate(all_files_data['white_games_opening'],
                       all_files_data['white_win_games_opening'],
                       white_win_rate_opening)
    calculate_win_rate(all_files_data['black_games_middle'],
                       all_files_data['black_win_games_middle'],
                       black_win_rate_middle)
    calculate_win_rate(all_files_data['white_games_middle'],
                       all_files_data['white_win_games_middle'],
                       white_win_rate_middle)
    calculate_win_rate(all_files_data['black_games_end'],
                       all_files_data['black_win_games_end'],
                       black_win_rate_end)
    calculate_win_rate(all_files_data['white_games_end'],
                       all_files_data['white_win_games_end'],
                       white_win_rate_end)
    calculate_win_rate(all_files_data['black_games'],
                       all_files_data['black_win_games'], black_win_rate)
    calculate_win_rate(all_files_data['white_games'],
                       all_files_data['white_win_games'], white_win_rate)
    '''
    write_moves_to_csv(
        black_move_probabilities_opening,
        r"E:\VSCPython\Amazons\result_csv\black_chess_moves_opening.csv")
    write_moves_to_csv(
        white_move_probabilities_opening,
        r"E:\VSCPython\Amazons\result_csv\white_chess_moves_opening.csv")
    write_moves_to_csv(
        black_move_probabilities_middle,
        r"E:\VSCPython\Amazons\result_csv\black_chess_moves_middle.csv")
    write_moves_to_csv(
        white_move_probabilities_middle,
        r"E:\VSCPython\Amazons\result_csv\white_chess_moves_middle.csv")
    write_moves_to_csv(
        black_move_probabilities_end,
        r"E:\VSCPython\Amazons\result_csv\black_chess_moves_end.csv")
    write_moves_to_csv(
        white_move_probabilities_end,
        r"E:\VSCPython\Amazons\result_csv\white_chess_moves_end.csv")
    write_moves_to_csv(
        black_move_probabilities,
        r"E:\VSCPython\Amazons\result_csv\black_chess_moves.csv")
    write_moves_to_csv(
        white_move_probabilities,
        r"E:\VSCPython\Amazons\result_csv\white_chess_moves.csv")
    '''
    write_win_rate_to_csv(
        black_win_rate_opening,
        r"E:\VSCPython\Amazons\result_csv\black_chess_win_rate_opening.csv")
    write_win_rate_to_csv(
        white_win_rate_opening,
        r"E:\VSCPython\Amazons\result_csv\white_chess_win_rate_opening.csv")
    write_win_rate_to_csv(
        black_win_rate_middle,
        r"E:\VSCPython\Amazons\result_csv\black_chess_win_rate_middle.csv")
    write_win_rate_to_csv(
        white_win_rate_middle,
        r"E:\VSCPython\Amazons\result_csv\white_chess_win_rate_middle.csv")
    write_win_rate_to_csv(
        black_win_rate_end,
        r"E:\VSCPython\Amazons\result_csv\black_chess_win_rate_end.csv")
    write_win_rate_to_csv(
        white_win_rate_end,
        r"E:\VSCPython\Amazons\result_csv\white_chess_win_rate_end.csv")
    write_win_rate_to_csv(
        black_win_rate,
        r"E:\VSCPython\Amazons\result_csv\black_chess_win_rate.csv")
    write_win_rate_to_csv(
        white_win_rate,
        r"E:\VSCPython\Amazons\result_csv\white_chess_win_rate.csv")

    print("所有文件已处理完成。")
