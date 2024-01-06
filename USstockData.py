import pandas as pd
from zipline.data import bundles
from zipline.data.data_portal import DataPortal
from zipline.utils.calendars import get_calendar

# Load the bundle
bundle = bundles.load('usstock-free-1min')

# Set the trading calendar
trading_calendar = get_calendar('NYSE')

# Create a DataPortal for accessing bundle data
data_portal = DataPortal(
    bundle.asset_finder,
    trading_calendar=trading_calendar,
    first_trading_day=bundle.equity_daily_bar_reader.first_trading_day,
    equity_minute_reader=bundle.equity_minute_bar_reader,
    equity_daily_reader=bundle.equity_daily_bar_reader,
    adjustment_reader=bundle.adjustment_reader,
)

# Specify the assets, dates, and fields you want to extract
assets = bundle.asset_finder.lookup_symbols(['AAPL', 'MSFT'], as_of_date=None)
start_date = pd.Timestamp('2023-01-01', tz='utc')
end_date = pd.Timestamp('2023-01-31', tz='utc')
fields = ['open', 'high', 'low', 'close', 'volume']

# Retrieve the data
df_list = []
for asset in assets:
    df = data_portal.get_history_window(
        assets=[asset],
        end_dt=end_date,
        bar_count=(end_date - start_date).days,
        frequency='1d',
        field=fields,
        data_frequency='daily'
    )
    df['symbol'] = asset.symbol
    df_list.append(df)

# Combine data for all assets into a single DataFrame
combined_df = pd.concat(df_list)

# Export the DataFrame to a CSV file
combined_df.to_csv('us_stock_data.csv')
