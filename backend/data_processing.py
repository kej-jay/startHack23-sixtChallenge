import requests as re
import math
import pandas as pd
import random
import json
import ast 
from datetime import datetime
from dateutil.relativedelta import relativedelta

API_KEY = "80dd5aada5mshca00e064c3a2194p117892jsn6c076829db74"
HOST = "holistic-finance-stock-data.p.rapidapi.com"
HEADERS = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": HOST
}


def get_risk(company):
    try:
        industry = json.loads(company)
        stock_symbol = industry[0]["ticker"]
        url = "https://holistic-finance-stock-data.p.rapidapi.com/api/v1/grade"
        querystring = {"symbol": stock_symbol}
        response = re.request("GET", url, headers=HEADERS, params=querystring)
        risk_assessments = response.json()
        ratings = []

        for risk in risk_assessments:
            if (risk["gradingCompany"] == "Goldman Sachs"):
                ratings.append(risk["newGrade"])
        scores = []

        for rating in ratings:
            if (rating == "Buy"):
                scores.append(3)
            elif (rating == "Neutral"):
                scores.append(2)
            elif (rating == "Sell"):
                scores.append(1)
            else:
                scores.append(0)

        return str(math.ceil(sum(scores)/len(scores)))
    except:

        return random.randrange(1, 4)


def get_random_company():
    df_companies = pd.read_csv("./data/company_sheet.csv", index_col=0)
    index = random.randint(0, len(df_companies)-1)
    entry = df_companies.iloc[[index]]

    return entry.to_json(orient="records")


def get_dates_volumes(company):
    df_ts = pd.read_csv("./data/company_listing_timeseries.csv", index_col=0)
    company = json.loads(company)
    company_name = company[0]["name"]
    comp_data = df_ts.loc[df_ts["company"]==company_name]
    dates = ast.literal_eval(comp_data["session_dates"].tolist()[0])
    dates = [datetime.strptime(x, '%Y-%m-%d').date() for x in dates]
    volumes = ast.literal_eval(comp_data["volume_values"].tolist()[0])
    dates_volumes = dict(zip(dates, volumes))

    return dates_volumes


def compute_popularity(company):
    dates_volumes = get_dates_volumes(company)
    last_month_dates = []
    last_month_volumes = []
    trend_indicator = ""
    for i in range(2,30):
        last_month_dates.append((datetime.now() - relativedelta(days=i)).date())
    for day in last_month_dates:
        if(day.weekday()==6 or day.weekday()==5):
            pass
        else:
            last_month_volumes.append(dates_volumes[day])
    last_month_avg = (sum(last_month_volumes)/len(last_month_volumes))
    yesterday = (datetime.now() - relativedelta(days=2)).date()
    volume_yesterday = dates_volumes[yesterday]
    trend = 100*(volume_yesterday/last_month_avg)

    if(trend>=90 and trend<=110):
        trend_indicator = "2"
    elif(trend<90):
        trend_indicator = "1"
    elif(trend>110):
        trend_indicator = "3"
    else:
        trend_indicator = "0"

    return trend_indicator


def get_dates_closes(company):
    df_ts = pd.read_csv("./data/company_listing_timeseries.csv", index_col=0)
    company = json.loads(company)
    company_name = company[0]["name"]
    comp_data = df_ts.loc[df_ts["company"]==company_name]
    dates = ast.literal_eval(comp_data["session_dates"].tolist()[0])
    dates = [datetime.strptime(x, '%Y-%m-%d').date() for x in dates]
    closes = ast.literal_eval(comp_data["close_values"].tolist()[0])
    dates_closes = dict(zip(dates,closes))
    return dates_closes


def compute_half_year_trend(company):
    dates_closes = get_dates_closes(company)
    trend_indicator = ""
    yesterday = (datetime.now() - relativedelta(days=2)).date()
    half_year_ago = (datetime.now() - relativedelta(weeks=26)).date()
    trend = 100*(dates_closes[yesterday]/dates_closes[half_year_ago])
    if(trend>=90 and trend<=110):
        trend_indicator = "2"
    elif(trend<90):
        trend_indicator = "1"
    elif(trend>110):
        trend_indicator = "3"
    else:
        trend_indicator = "0"

    return trend_indicator


def get_category_industry(company):
    cat_df = pd.read_csv("./data/category_industry.csv")
    industry = json.loads(company)
    industry_name = industry[0]["sector"]
    category_df = cat_df[cat_df["Industry"]==industry_name]
    category_name = category_df["Category"].tolist()[0]

    return category_name


def get_description(company):
    try:
        company = json.loads(company)
        ticker = company[0]["ticker"]
        url = "https://holistic-finance-stock-data.p.rapidapi.com/api/v1/profile"
        querystring = {"symbol":ticker}
        response = re.request("GET", url, headers=HEADERS, params=querystring)
        company_details = response.json()[0]
        company_description = company_details["description"]

        return company_description
    except:

        return "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."


def get_short_description(description,max_length):
    splitted = description.split(".")
    max_length = max_length
    current_length = 0
    parts = []
    for split in splitted:
        current_length += len(split)
        parts.append(split)
        if current_length>max_length:
            break
    short_description = ".".join(parts)

    return short_description
