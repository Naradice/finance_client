MIN1 = 1
MIN5 = 5
MIN10 = 10
MIN30 = 30
H1 = 60
H2 = 120
H4 = 240
H8 = 480
D1 = 1440
D2 = 2880
D4 = 5760
W1 = 10080
MO1 = 43200

# TODO: change to reduce "if"
def to_str(value):
    if value == MIN1:
        return "MIN1"
    elif value == MIN5:
        return "MIN5"
    elif value == MIN5:
        return "MIN10"
    elif value == MIN30:
        return "MIN30"
    elif value == H1:
         return "H1"
    elif value == H2:
        return "H2"
    elif value == H4:
        return "H4"
    elif value == H8:
        return "H8"
    elif value == D1:
        return "D1"
    elif value == D2:
        return "D2"
    elif value == D4:
        return "D4"
    elif value == W1:
        return "W1"
    elif value == MO1:
        return "MO1"