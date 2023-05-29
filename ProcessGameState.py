import math

import pandas as pd
import pyarrow
import shapely.geometry.polygon
from tqdm import tqdm
from collections import defaultdict
from shapely.geometry import Point

from shapely.geometry.polygon import Polygon
import matplotlib.pyplot as plt

player_color = {"Player0":"blue","Player1":"red","Player2":"green","Player3":"purple","Player4":"orange","Player5":"pink","Player6":"black","Player7":"yellow","Player8":"cyan","Player9":"brown"}


def get_second_from_clock_timer(clock_time: str):
    time = list(map(lambda x: int(x), clock_time.split(":")))
    second = 0
    if time[0] != 1:
        second += 55
        second += 60-time[1]
    else:
        second += 55-time[1]
    return second


class ProcessGameState:
    dataframe = []
    polygon = Polygon()
    polygon_square = Polygon()
    hashmapx = defaultdict(list)
    hashmapy = defaultdict(list)
    hashmapz = defaultdict(list)
    cache_dataframe = []

    def __init__(self, file: str):
        """
        :param file: filename for input file
        Also make a deep copy from original dataframe for further use called cache dataframe
        """
        self.dataframe = pd.read_parquet(file, engine="pyarrow")
        self.cache_dataframe = self.dataframe.copy(deep=True)

    def extract_weapon(self):
        """
        As mentioned in design document, this function will extract weapon_class from inventory and replace the
        raw data saved in the cache_dataframe. Try to filter the data as much before call this function to reduce
        process time.
        """
        for idx, row in tqdm(self.cache_dataframe.iterrows(), total=self.cache_dataframe.shape[0],
                             desc="Extracting Data From File"):
            if row['inventory'] is not None:
                self.cache_dataframe.at[idx, 'inventory'] = [i['weapon_class'] for i in row['inventory']]

    def extract_xyz(self):
        """
        This function will extract x,y,z data from cache_dataframe and doing an inverted index on them.
        Take x data as an example, The {x:[index]} i

        """
        for idx, row in tqdm(self.cache_dataframe.iterrows(), total=self.cache_dataframe.shape[0],
                             desc="Extracting Data From File"):
            self.hashmapx[int(row['x'])].append(idx)
            self.hashmapy[int(row['y'])].append(idx)
            self.hashmapz[int(row['z'])].append(idx)

    def filter_by_team_name(self, team: str):
        self.cache_dataframe = self.cache_dataframe[self.cache_dataframe["team"] == team]

    def filter_by_side(self, side: str):
        self.cache_dataframe = self.cache_dataframe[self.cache_dataframe["side"] == side]

    def filter_by_alive(self, alive: str):
        self.cache_dataframe = self.cache_dataframe[self.cache_dataframe["is_alive"] == alive]

    def filter_by_area_name(self, area_name: str):
        self.cache_dataframe = self.cache_dataframe[self.cache_dataframe["area_name"] == area_name]

    def filter_by_player(self,player: str):
        self.cache_dataframe = self.cache_dataframe[self.cache_dataframe["player"] == player]

    def reset_cache(self):
        self.cache_dataframe = self.dataframe.copy(deep=True)
        self.hashmapx = defaultdict(list)
        self.hashmapy = defaultdict(list)
        self.hashmapz = defaultdict(list)

    def split_row_by_round_and_player(self):
        return self.cache_dataframe.groupby(["round_num", "player"])

    def calculate_avg_time(self):
        group = self.split_row_by_round_and_player()
        keys = list(group.groups.keys())
        dictionary = defaultdict(list)
        for i in keys:
            dictionary[i[0]].append(i[1])
        total_time = []
        for round_num, players in dict.items():
            time = []
            weapon_counter = 0
            for player in players:
                game = group.get_group((round_num, player))
                for index, row in game.iterrows():
                    if "SMG" in row['inventory'] or "Rifle" in row['inventory']:
                        time.append(get_second_from_clock_timer(row['clock_time']))
                        weapon_counter += 1
                        break
            if weapon_counter > 1:
                time.sort()
                total_time.append(time[1])
            print(round_num, time)
        return int(sum(total_time)/len(total_time))

    def plot_point(self):
        for idx,row in self.cache_dataframe.iterrows():
            point = Point(row['x'], row['y'])
            x, y = point.xy
            plt.scatter(x, y, color=player_color[row['player']])
        plt.show()

    def check_if_any_row_in_boundary(self, boundary_xy: list, boundary_z: list):
        self.extract_xyz()
        self.polygon = shapely.geometry.polygon.Polygon(boundary_xy)
        x_min = y_min = z_min = math.inf
        x_max = y_max = z_max = -math.inf

        for i in boundary_xy:
            x_min = min(i[0], x_min)
            x_max = max(i[0], x_max)
            y_min = min(i[1], y_min)
            y_max = max(i[1], y_max)

        z_max = boundary_z[1]
        z_min = boundary_z[0]
        x_set = set()
        y_set = set()
        z_set = set()

        for i in range(y_min, y_max+1):
            if i in self.hashmapy:
                y_set.update(self.hashmapy[i])

        if len(y_set) == 0:
            return []

        for i in range(x_min, x_max+1):
            if i in self.hashmapx:
                x_set.update(self.hashmapx[i])

        if len(x_set) == 0:
            return []

        x_set = x_set.intersection(y_set)

        if len(x_set) == 0:
            return []

        for i in range(z_min, z_max+1):
            if i in self.hashmapz:
                z_set.update(self.hashmapz[i])

        if len(z_set) == 0:
            return []

        z_set = z_set.intersection(x_set)
        if len(z_set) == 0:
            return []

        self.polygon = Polygon(boundary_xy)
        point_list = []
        fig, ax = plt.subplots()
        x, y = self.polygon.exterior.xy
        ax.plot(x, y, color="blue")
        for i in z_set:
            row = self.dataframe.iloc[i]
            point = Point(row['x'], row['y'])
            if self.polygon.contains(point):
                point_list.append(i)

                x, y = point.xy
                plt.scatter(x, y, color=player_color[row['player']])
        plt.show()
        self.cache_dataframe = self.cache_dataframe.loc[point_list]

        return point_list

    def generate_heatmap(self):
        x_list = []
        y_list = []
        x_min = y_min = math.inf
        x_max = y_max = -math.inf
        for idx, row in self.cache_dataframe.iterrows():
            x = int(row['x'])//20*20
            y = int(row['y'])//20*20
            x_min = min(x, x_min)
            x_max = max(x, x_max)
            y_min = min(y, y_min)
            y_max = max(y, y_max)
            x_list.append(x)
            y_list.append(y)
        fig1 = plt.figure()
        plt.hist2d(x_list, y_list, bins=20)
        plt.xlabel('x')
        plt.ylabel('y')
        cbar = plt.colorbar()
        cbar.ax.set_ylabel('Counts')
        plt.show()

    def write_to_csv(self, file: str):
        self.cache_dataframe.to_csv(file)
