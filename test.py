import heapq
from typing import List, Tuple, Optional
import numpy as np
import pandas as pd

DIRECTIONS = [
    (-1, 0, 1), (1, 0, 1), (0, -1, 1), (0, 1, 1),
    (-1, -1, 4), (-1, 1, 4), (1, -1, 4), (1, 1, 4)
]

def heuristic(current: Tuple[int, int], goal: Tuple[int, int]) -> float:
    """Hàm heuristic: khoảng cách Manhattan (có cải tiến)."""
    dx, dy = abs(current[0] - goal[0]), abs(current[1] - goal[1])
    return min(dx, dy) * 1 + abs(dx - dy) * 1


def astar_pathfinding(grid_size: Tuple[int, int],
                      start: Tuple[int, int],
                      goal: Tuple[int, int],
                      obstacles: List[Tuple[int, int]]
                      ) -> Tuple[Optional[List[Tuple[int, int]]], float]:

    rows, cols = grid_size
    obstacle_set = set(obstacles)

    open_list = [(0, 0, start, [start])]
    heapq.heapify(open_list)

    g_scores = {start: 0}
    f_scores = {start: heuristic(start, goal)}

    while open_list:
        _, current_g, current_pos, path = heapq.heappop(open_list)

        if current_pos == goal:
            return path, current_g

        for dx, dy, cost in DIRECTIONS:
            neighbor = (current_pos[0] + dx, current_pos[1] + dy)

            if 1 <= neighbor[0] <= rows and 1 <= neighbor[1] <= cols:
                if neighbor in obstacle_set:
                    continue

                tentative_g = current_g + cost
                if neighbor not in g_scores or tentative_g < g_scores[neighbor]:
                    g_scores[neighbor] = tentative_g
                    f = tentative_g + heuristic(neighbor, goal)
                    new_path = path + [neighbor]
                    heapq.heappush(open_list, (f, tentative_g, neighbor, new_path))

    return None, float('inf')


def print_grid(grid_size: Tuple[int, int],
               start: Tuple[int, int],
               goal: Tuple[int, int],
               obstacles: List[Tuple[int, int]],
               path: Optional[List[Tuple[int, int]]]) -> None:

    rows, cols = grid_size
    grid = np.full((rows + 1, cols + 1), '.', dtype=str)

    for obs in obstacles:
        grid[obs] = 'X'

    if path:
        for pos in path:
            if pos != start and pos != goal:
                grid[pos] = '*'

    grid[start] = 'A'
    grid[goal] = 'B'

    df = pd.DataFrame(
        grid[1:, 1:],
        index=[f"H{i}" for i in range(1, rows + 1)],
        columns=[f"C{j}" for j in range(1, cols + 1)]
    )

    print("\nMa trận đường đi:")
    print(df)


def main():
    
    grid_size = (6, 11)
    start = (5, 8)
    goal = (2, 5)
    obstacles = [
        (2, 4), (3, 4), (3, 5), (3, 6), (3, 7), (3, 8)
    ]

    path, total_cost = astar_pathfinding(grid_size, start, goal, obstacles)

    if path:
        print("✅ Tìm thấy đường đi:")
        print("→ Đường đi ngắn nhất:", path)
        print("→ Chi phí tổng:", total_cost)
    else:
        print("❌ Không tìm thấy đường đi!")

    print_grid(grid_size, start, goal, obstacles, path)

if __name__ == "__main__":
    main()
