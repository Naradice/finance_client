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
    MIN1: "MIN1",
    MIN5: "MIN5",
    MIN10: "MIN10",
    MIN30: "MIN30",
    H1: "H1",
    H2: "H2",
    H4: "H4",
    H8: "H8",
    D1: "D1",
    D2: "D2",
    D4: "D4",
    W1: "W1",
    MO1: "MO1",
}

freq_str = {
    MIN1: "1min",
    MIN5: "5min",
    MIN10: "10min",
    MIN30: "30min",
    H1: "1H",
    H2: "2H",
    H4: "4H",
    H8: "8H",
    D1: "1D",
    D2: "2D",
    D4: "4D",
    W1: "W1",
    MO1: "MO1",
}


def to_panda_freq(minutes: int):
    try:
        return freq_str[minutes]
    except Exception:
        hours = minutes / 60
        if hours >= 1:
            days = hours / 24
            if days >= 1:
                week = days / 7
                if week >= 1:
                    month = days / 30
                    if month >= 1:
                        years = minutes / (60 * 24 * 365)
                        if years >= 1:
                            return f"{str(int(years))}Y"
                        else:
                            return f"{str(int(month))}M"
                    else:
                        return f"{str(int(week))}W"
                else:
                    return f"{str(int(days))}D"
            else:
                return f"{str(int(hours))}h"
        else:
            return f"{str(int(minutes))}min"


def to_str(value: int):
    if value in min_str:
        return min_str[value]
    return str(value)
