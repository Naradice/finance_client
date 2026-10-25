"""Recognize actionable API failures without exposing provider payloads or credentials."""


def ai_service_error(error):
    code = getattr(error, "code", None)
    if code == "credit_balance_exhausted":
        return "AI API credit balance is exhausted (credit_balance_exhausted). Check API billing."
    if code == "insufficient_quota":
        return "AI API usage quota is unavailable (insufficient_quota). Check API billing and limits."
    return None
