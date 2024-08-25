from datetime import date, timedelta


# Use current day to get previous month and year based on current date
def get_previous_month_date():
    current_date = date.today()
    previous_month_date = current_date.replace(day=1) - timedelta(days=1)
    return previous_month_date


PREV_YEAR_MONTH = (
    f"{str(get_previous_month_date().year)}-"
    f"{str(get_previous_month_date().month).zfill(2)}"
)
