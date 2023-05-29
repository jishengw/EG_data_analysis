import ProcessGameState

def question1():

    state = ProcessGameState.ProcessGameState("game_state_frame_data.parquet")
    state.filter_by_side("T")
    state.filter_by_team_name("Team2")
    boundary_xy = [[-1735, 250], [-2024, 398], [-2806, 742], [-2472, 1233], [-1565, 580]]
    boundary_z = [285, 421]
    state.check_if_any_row_in_boundary(boundary_xy,boundary_z)
    state.write_to_csv("result1.csv")
    state.reset_cache()
def question2():
    state = ProcessGameState.ProcessGameState("game_state_frame_data.parquet")
    state.filter_by_side("T")
    state.filter_by_team_name("Team2")
    state.filter_by_area_name("BombsiteB")
    state.filter_by_alive(True)
    state.extract_weapon()
    print(state.calculate_avg_time())
    state.write_to_csv("result2.csv")
    state.reset_cache()

def question3():
    state = ProcessGameState.ProcessGameState("game_state_frame_data.parquet")
    state.filter_by_side("CT")
    state.filter_by_team_name("Team2")
    state.filter_by_area_name("BombsiteB")
    state.generate_heatmap()
    state.plot_point()
    # state.plot_point()
    # boundary_xy = [[-5000,-5000],[5000,-5000],[5000,5000][-5000,5000],]
    # boundary_z = [-500,500]
    # state.check_if_any_row_in_boundary(boundary_xy,boundary_z)

question3()
