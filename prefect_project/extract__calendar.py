from importlib.resources import path
import pandas as pd
import sqlalchemy as sa
import datetime
import workalendar.europe as workalendar
from datetime import datetime, timedelta
from prefect import flow, task
from generic_tasks import write_raw


@task
def generate_calendar():
    current_year = datetime.today().year
    years = list(range(current_year - 5, current_year + 2, 1))

    df_calendar = pd.date_range(start=datetime(years[0], 1, 1), end=datetime(years[-1], 12, 31)).to_frame(index=False, name='date_actual')

    year_holidays_list = []
    for year in years:
        year_holidays_list.append(pd.DataFrame(workalendar.Denmark().holidays(year),  columns=['date_actual', 'holiday']))

    df_holidays = pd.concat(year_holidays_list)

    df_holidays['date_actual'] = pd.to_datetime(df_holidays['date_actual'])

    df_calendar['day_actual'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%-d'))
    df_calendar['day_zero_added'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%d'))

    df_calendar['weekday_actual'] = df_calendar['date_actual'].apply(lambda y: y.isoweekday())
    df_calendar['weekday_name_short'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%a'))
    df_calendar['weekday_name_long'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%A'))

    df_calendar['month_actual'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%-m'))
    df_calendar['month_zero_added'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%m'))
    df_calendar['month_name_short'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%b'))
    df_calendar['month_name_long'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%B'))

    df_calendar['quarter_actual'] = df_calendar['date_actual'].dt.quarter
    df_calendar['quarter_name'] = 'Q' + df_calendar['date_actual'].dt.quarter.astype(str)
    df_calendar['year_quarter_name'] = df_calendar['date_actual'].dt.to_period('Q').astype(str)

    df_calendar['day_of_year'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%j')).astype(int)
    df_calendar['iso_week_of_year'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%V'))
    df_calendar['iso_year'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%G'))
    df_calendar['year'] = df_calendar['date_actual'].apply(lambda y: datetime.strftime(y, '%Y'))
    
    df_calendar['first_day_of_month'] = df_calendar['date_actual'].apply(lambda y: y.replace(day=1))
    df_calendar['end_of_month'] = df_calendar['date_actual'].apply(lambda y: (y.replace(day=1) + timedelta(days=31)).replace(day=1) + timedelta(days=-1))
    df_calendar['first_day_of_year'] = df_calendar['date_actual'].apply(lambda y: y.replace(month= 1, day=1))

    df_calendar['is_working_day'] = df_calendar['date_actual'].apply(lambda y: workalendar.Denmark().is_working_day(y))

    df_calendar = df_calendar.merge(df_holidays, on='date_actual', how='left')
    # print types
    return df_calendar

@flow
def extract__calendar():
    calendar = generate_calendar()
    write_raw(calendar, 'calendar/calendar')    
     
if __name__ == "__main__":
    extract__calendar()



