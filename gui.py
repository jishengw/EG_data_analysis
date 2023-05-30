# import tkinter as tk
# button = tk.Button(
#     text="upload"
# )
# window = tk.Tk()
# button.pack()
# window.mainloop()
from tkinter import *
r = Tk()
r.geometry("500x200")
button = Button(r, text='Upload parquet', width=25)
button.pack()
label = Label(text="Choose Team")
label.pack()
variable = StringVar(r)
variable.set("Both Team") # default value
w = OptionMenu(r, variable, "Team1", "Team2", "Both")
w.pack()

label = Label(text="Choose Player")
label.pack()
variable = StringVar(r)
variable.set("All Players")
w = OptionMenu(r, variable, "Player0", "Player1", "Player2","Player3")
w.pack()

label = Label(text="Choose Round")
label.pack()
variable = StringVar(r)
variable.set("All Round")
w = OptionMenu(r, variable, "Round 1", "Round 2", "Round 3","Round 4")
w.pack()

label = Label(text="At least")
text=Text(r,width=5)
variable = StringVar(r)
variable.set("Weapon")
w = OptionMenu(r, variable, "Rifle", "Pistol", "SMG")
label.pack(side=LEFT)
text.pack(side=LEFT)
w.pack(side=LEFT)

r.mainloop()