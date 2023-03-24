import base64
import io
import matplotlib
matplotlib.use('Agg')
from pathlib import Path
from string import ascii_lowercase
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import ast
import SIX_API


data_object = SIX_API.FinancialDataAPI()


def convert_string_to_list_of_strings(string_list):
    re_list = ast.literal_eval(string_list)

    return re_list


def convert_string_to_list_of_numeric(string_list):
    re_list = ast.literal_eval(string_list)

    return [float(x) for x in re_list]


"""
    Operation:
            Scrappes for companies listed in NASDAQ or NYSE and stores them as a csv
            CSV contains columns:
                    + name          
                    + sector        
                    + market      
                    + market code 
                    + ticker
    Input:
            None
    Output:
            Doesn't return something - saves a dataframe as csv
"""


def scrape_companies():
    # Initializing lists for dataframe object
    company_names = []
    company_sectors = []
    most_liquid_markets_market = []
    most_liquid_markets_bc = []
    most_liquid_listing_tickers = []

    # Iterating alphabet letters
    for letter in ascii_lowercase:

        # Querying every letter
        compnay_search_obj = data_object.text_search(letter)

        # Check that all required fields are there
        for i in range(len(compnay_search_obj.data.searchInstruments)):
            if (
                    'issuer' in compnay_search_obj.data.searchInstruments[i].hit.__dict__.keys()
                and
                    'mostLiquidMarket' in compnay_search_obj.data.searchInstruments[i].hit.__dict__.keys()
                and
                    'mostLiquidListing' in compnay_search_obj.data.searchInstruments[i].hit.__dict__.keys()
            ):

                # Checl that the Market is NYSE or NASDAQ
                if (
                        compnay_search_obj.data.searchInstruments[i].hit.mostLiquidMarket.name == 'NASDAQ'
                    or
                        compnay_search_obj.data.searchInstruments[i].hit.mostLiquidMarket.name == 'NYSE'
                ) and (
                    '.' not in compnay_search_obj.data.searchInstruments[i].hit.mostLiquidListing.ticker
                ):

                    # Storing relevant info: Company name, sector, market, market code and ticker
                    company_names.append(
                        compnay_search_obj.data.searchInstruments[i].hit.issuer.name
                    )
                    company_sectors.append(
                        " ".join(compnay_search_obj.data.searchInstruments[i].hit.issuer.sector.split("_")).title() if " ".join(compnay_search_obj.data.searchInstruments[i].hit.issuer.sector.split("_")).title() != "Internet Software And It Services" else "Internet Software And IT Services"
                    )
                    most_liquid_markets_market.append(
                        compnay_search_obj.data.searchInstruments[i].hit.mostLiquidMarket.name
                    )
                    most_liquid_markets_bc.append(
                        compnay_search_obj.data.searchInstruments[i].hit.mostLiquidMarket.bc
                    )
                    most_liquid_listing_tickers.append(
                        compnay_search_obj.data.searchInstruments[i].hit.mostLiquidListing.ticker
                    )

    company_sheet = pd.DataFrame(
        data={
            'name': company_names,
            'sector': company_sectors,
            'market': most_liquid_markets_market,
            'market_code': most_liquid_markets_bc,
            'ticker': most_liquid_listing_tickers
        }
    )

    company_sheet.to_csv(Path(__file__).parent.joinpath('data').joinpath('company_sheet.csv'))


"""
    Operation:
            Scrappes the SIX API for timeseries of company listings from the previous function
            Saves everything in a csv. CSV contains columns:
                    + Currency       -   single string value
                    + open_values    -   list of all opening values for a year
                    + close-values   -   list of all closing values for a year
                    + low_values     -   list of all low values for a year
                    + high_values    -   list of all high values for a year
                    + volume_values  -   volume traded for a year
    Input:
            The start date for the time series
    Output:
            Doesn't return something - saves a dataframe as csv
"""


def scrape_timeseries_data(start_date):
    # Company csv for scrapping all stored companies' timeseries data on listings
    company_sheet_df = pd.read_csv(Path(__file__).parent.joinpath('data').joinpath('company_sheet.csv'))

    # Initializing lists for dataframe object
    all_companies = []
    all_currency = []
    all_session_dates = []
    all_open_values = []
    all_close_values = []
    all_low_values = []
    all_high_values = []
    all_volume_values = []

    # Iterating over the dataframe
    for index, row in company_sheet_df.iterrows():
        # Querying for the time series data
        listing_search_object = data_object.listing_EoDTimeseries(
            "TICKER_BC",
            [f"{row['ticker']}_{row['market_code']}"],
            start_date
        )

        # Initializing lists for dataframe object
        session_dates = []
        open_values = []
        close_values = []
        low_values = []
        high_values = []
        volume_values = []
        currency = listing_search_object.data.listings[0].lookup.listingCurrency

        for daily_session in listing_search_object.data.listings[0].marketData.eodTimeseries:
            if (
                    'open' in daily_session.__dict__.keys()
                and
                    'close' in daily_session.__dict__.keys()
                and
                    'high' in daily_session.__dict__.keys()
                and
                    'low' in daily_session.__dict__.keys()
            ):
                session_dates.append(daily_session.sessionDate)
                open_values.append(daily_session.open)
                close_values.append(daily_session.close)
                low_values.append(daily_session.low)
                high_values.append(daily_session.high)

                if 'volume' in daily_session.__dict__.keys():
                    volume_values.append(daily_session.volume)

        all_companies.append(row['name'])
        all_currency.append(currency)
        all_session_dates.append(session_dates)
        all_open_values.append(open_values)
        all_close_values.append(close_values)
        all_low_values.append(low_values)
        all_high_values.append(high_values)
        all_volume_values.append(volume_values)

    company_listing_timeseries = pd.DataFrame(
        data={
            'company': all_companies,
            'currency': all_currency,
            'session_dates': all_session_dates,
            'open_values': all_open_values,
            'close_values': all_close_values,
            'low_values': all_low_values,
            'high_values': all_high_values,
            'volume_values': all_volume_values
        }
    )

    company_listing_timeseries.to_csv(Path(__file__).parent.joinpath('data').joinpath('company_listing_timeseries.csv'))


def get_open_close_high_low_volume_lists(company_name):
    # Loading the timeseries dataframe
    timeseries_df = pd.read_csv(
        Path(__file__).parent.joinpath('data').joinpath('company_listing_timeseries.csv'),
        index_col = 0
    )

    # Selecting the appropriate row
    company_data = timeseries_df.loc[timeseries_df['company'] == company_name]

    currency = company_data['currency'].values[0]
    session_dates_series = convert_string_to_list_of_strings(company_data['session_dates'].values[0])
    open_values_series = convert_string_to_list_of_numeric(company_data['open_values'].values[0])
    low_values_series = convert_string_to_list_of_numeric(company_data['close_values'].values[0])
    high_values_series = convert_string_to_list_of_numeric(company_data['high_values'].values[0])
    if company_data['volume_values'].values[0] != '[]':
        volume_values_series = convert_string_to_list_of_numeric(company_data['volume_values'].values[0])
    else:
        volume_values_series = None

    return currency, session_dates_series, open_values_series, low_values_series, high_values_series, volume_values_series


def create_graph(sessions: list, series_data: list, graph_name, company_name, currency, x_axis_name, y_axis_name):
    if len(series_data) == 1:
        plot_data = pd.DataFrame(
            data={
                'dates': sessions,
                'time_series': series_data[0]
            }
        )

        ax = sns.lineplot(data=plot_data, x='dates', y='time_series')
        ax.xaxis.set_major_locator(plt.MaxNLocator(4))
        ax.set(xlabel=x_axis_name, ylabel=f'{y_axis_name} ({currency})', title=f"{graph_name} for {company_name}")
        ax.yaxis.get_major_formatter().set_scientific(False)
        ax.yaxis.get_major_formatter().set_useOffset(False)
        plt.tight_layout()
        plt.grid()

    elif len(series_data) == 2:
        plot_data = pd.DataFrame(
            data={
                'dates': sessions[-14:-1],
                'lowest price': series_data[0][-14:-1],
                'highest price': series_data[1][-14:-1]
            }
        )

        ax = sns.lineplot(data=pd.melt(plot_data, ['dates']), x='dates', y='value', hue='variable')
        ax.xaxis.set_major_locator(plt.MaxNLocator(4))
        ax.set(xlabel=x_axis_name, ylabel=f'{y_axis_name} ({currency})', title=f"{graph_name} for {company_name}")
        ax.yaxis.get_major_formatter().set_scientific(False)
        ax.yaxis.get_major_formatter().set_useOffset(False)
        plt.tight_layout()
        plt.grid()

    with io.BytesIO() as bytes_image:
        plt.savefig(bytes_image, format='jpeg')
        data = base64.b64encode(bytes_image.getbuffer()).decode("ascii")

    io.BytesIO().close()
    plt.close()

    return data
