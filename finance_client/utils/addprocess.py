from .indicaters.economic import SP500

__indicaters = {
    "SP500": SP500
}

def get_indicater(key, start, end):
    if key in __indicaters:
        func = __indicaters[key]
        return func(start=start, end=end)