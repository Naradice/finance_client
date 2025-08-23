def to_panda_freq(minutes: int):
    hours = minutes / 60
    if hours >= 1:
        days = hours / 24
        if days >= 1:
            week = days / 7
            if week >= 1:
                month = days / 30
                if month >= 1:
                    years = minutes / (60 * 24 * 365)
                    return f"{str(int(years))}Y"
                else:
                    return f"{str(int(month))}M"
            else:  # 15 days are handled as 2 weeks
                return f"{str(int(week))}W"
        else:
            return f"{str(int(hours))}h"
    else:
        return f"{str(int(minutes))}MIN"
