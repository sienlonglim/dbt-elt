from datetime import date, timedelta


def get_previous_month_date(date_input: date) -> date:
    previous_month_date = date_input.replace(day=1) - timedelta(days=1)
    return previous_month_date


def format_month_date(date_input: date) -> str:
    return f"{date_input.year}-{str(date_input.month).zfill(2)}"


HISTORICAL_START_YEAR_MONTH = "2018-01"
end_year_month = format_month_date(get_previous_month_date(date.today()))

YEAR_MONTHS_TO_EXTRACT = []
while end_year_month >= HISTORICAL_START_YEAR_MONTH:
    YEAR_MONTHS_TO_EXTRACT.append(end_year_month)
    previous_month_date = get_previous_month_date(
        date(year=int(end_year_month[:4]), month=int(end_year_month[5:]), day=1)
    )
    end_year_month = format_month_date(previous_month_date)
