from datetime import datetime

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

# string value to use it for file name, etc
min_str = {
    MIN1: "MIN1",
    MIN5: "MIN5",
    MIN10: "MIN10",
    MIN15: "MIN15",
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

# freq value used with pandas
freq_str = {
    MIN1: "1min",
    MIN5: "5min",
    MIN10: "10min",
    MIN15: "15min",
    MIN30: "30min",
    H1: "1h",
    H2: "2h",
    H4: "4h",
    H8: "8h",
    D1: "1D",
    D2: "2D",
    D4: "4D",
    W1: "W1",
    MO1: "MO1",
    None: None,
}


def to_str(value: int):
    if value in min_str:
        return min_str[value]
    return str(value)


def to_freq(freq_str_value: str):
    for freq, key in freq_str.items():
        if key == freq_str_value:
            return freq
    for freq, key in min_str.items():
        if key == freq_str_value:
            return freq


def to_freq_str(mins: int):
    if mins in freq_str:
        return freq_str[mins]
    if mins < 60:
        return f"{mins}min"
    if mins < 60 * 24:
        if mins % 60 == 0:
            return f"{mins // 60}h"
    if mins < 60 * 24 * 7:
        if mins % 60 == 0:
            if mins % (60 * 24) == 0:
                return f"{mins // (60*24)}D"
            else:
                return f"{mins // 60}h"
    return f"{mins}min"


def get_frame_time(time: datetime, frame):
    frame = int(frame)
    year, month, day, hour, minute = time.year, time.month, time.day, time.hour, time.minute
    tz = time.tzinfo

    if frame <= H1:
        frame_minute = (minute // frame) * frame
    elif frame <= D1:
        frame_minute = ((hour * 60 + minute) // frame) * frame
        hour = frame_minute // 60
        frame_minute = frame_minute % 60
    elif frame <= W1:
        frame_minute = ((day * 24 * 60 + hour * 60 + minute) // frame) * frame
        day = frame_minute // (24 * 60)
        frame_minute = frame_minute % (24 * 60)
        hour = frame_minute // 60
        frame_minute = frame_minute % 60
    else:
        frame_minute = ((month * 31 * 24 * 60 + day * 24 * 60 + hour * 60 + minute) // frame) * frame
        month = frame_minute // (31 * 24 * 60) + 1
        # assume monthly. Don' care 45 deys etc.
        day = 1
        hour = 0
        frame_minute = 0

    frame_time = datetime(year, month, day, hour, frame_minute, tzinfo=tz)

    return frame_time
