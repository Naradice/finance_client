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


def to_str(value: int):
    if value in min_str:
        return min_str[value]
    return str(value)


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
