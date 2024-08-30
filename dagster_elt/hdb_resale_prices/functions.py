from datetime import date, timedelta


def get_previous_month_date(date_input: date) -> date:
    previous_month_date = date_input.replace(day=1) - timedelta(days=1)
    return previous_month_date


def format_month_date(date_input: date) -> str:
    return f"{date_input.year}-{str(date_input.month).zfill(2)}"


def build_list_of_year_month_strings(
    historical_start: str
) -> list[str]:
    """
    Builds a list of YYYY-MM strings based on starting year up till the month before today
    """
    year_and_months = []
    end_year_month = format_month_date(get_previous_month_date(date.today()))
    while end_year_month >= historical_start:
        year_and_months.append(end_year_month)
        previous_month_date = get_previous_month_date(
            date(year=int(end_year_month[:4]), month=int(end_year_month[5:]), day=1)
        )
        end_year_month = format_month_date(previous_month_date)
    return year_and_months