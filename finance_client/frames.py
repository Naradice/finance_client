MIN1 = 1
MIN5 = 5
MIN10 = 10
MIN15 = 15
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

min_str = {
    MIN1:"MIN1",MIN5:"MIN5",MIN10:"MIN10",MIN30:"MIN30",
    H1:"H1",H2:"H2",H4:"H4",H8:"H8",
    D1:"D1",D2:"D2",D4:"D4",
    W1:"W1",
    MO1:"MO1"
}

# TODO: change to reduce "if"
def to_str(value:int):
    if value in min_str:
        return min_str[value]
    return str(value)
    