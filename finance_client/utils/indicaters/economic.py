def SP500(start=None, end=None, provider="fred"):
    if provider == "fred":
        from .fred import get_SP500
        return get_SP500(start, end)
    else:
        return None