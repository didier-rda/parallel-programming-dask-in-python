'''
A DataFrame of daily weather data from The Weather Underground is provided for you as df. Your job is to determine the exact amount of memory used (in kilobytes) to store the DataFrame in entirety.

You can use either df.info() or df.memory_usage() to determine this value. Remember that df.memory_usage() will return memory usage in bytes and the value must be divided by 1024 to convert to kilobytes (KB). This DataFrame has at least one column of text.
'''

import pandas as pd
import numpy as np
df = pd.DataFrame().from_dict({'Rod':['Didier']})

df.info()

'''
NumPy transformations
Many NumPy transformations, while fast, use one or more temporary arrays. Therefore, those transformations require more storage than the original array required.

An array of temperature values in Celsius is provided for you as celsius. Your job is to monitor memory consumption while applying NumPy vectorized operations. The data comes from The Weather Underground.

The function memory_footprint() has been provided for you to return the total amount of memory (in megabytes or MB) currently in use by your program. This function uses the psutil and os modules. You can find the function definition in the course appendix.
'''

celsius = np.array([0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5], dtype=float)

# Print the size in MB of the celsius array
print(celsius.nbytes / 1024**2)

# Call memory_footprint(): before
before = memory_footprint()

# Convert celsius by multiplying by 9/5 and adding 32: fahrenheit
fahrenheit = celsius * 9/5 + 32

# Call memory_footprint(): after
after = memory_footprint()

'''
Filtering WDI data in chunks
Using the World Bank's World Development Indicators (or WDI) dataset, you're going to plot the percentage of the population of Australia in urban centers since 1980.

Your job is to loop over chunks of the WDI dataset; from each chunk, you will filter out rows describing Australia's "percent urban population." You'll then concatenate the filtered chunks and plot the results. pandas has been pre-imported for you as pd.
'''

# Create empty list: dfs
dfs = []

# Loop over 'WDI.csv'
for chunk in pd.read_csv('WDI.csv', chunksize=1000):
    # Create the first Series
    is_urban = chunk['Indicator Name']=='Urban population (% of total)'
    # Create the second Series
    is_AUS = chunk['Country Code']=='AUS'

    # Create the filtered chunk: filtered
    filtered = chunk.loc[is_urban & is_AUS]

    # Append the filtered chunk to the list dfs
    dfs.append(filtered)

'''
Concatenating & plotting WDI data
In the previous exercise, you read a large CSV file by chunks, performed boolean filtering on each chunk, and stored each filtered chunk in a list. Before filtering, each chunk contained up to 1000 rows. However, after filtering, some of the filtered chunks had no rows. You'll now use len() to determine the number of chunks and the actual number of rows that the filter retains.

Your job is to use pd.concat() to make a single DataFrame from the list dfs; this list is provided for you. The function pd.concat() will take the list of DataFrames and concatenate them into a new DataFrame object. Finally, you will plot the results. The modules matplotlib.pyplot and pandas have been imported for you with standard aliases (plt and pd respectively).
'''

# Print length of list dfs
print(len(dfs))

# Apply pd.concat to dfs: df
df = pd.concat(dfs)

# Print length of DataFrame df
print(len(df))

# Call df.plot.line with x='Year' and y='value'
df.plot.line(x='Year', y='value')
plt.ylabel('% Urban population')

# Call plt.show()
plt.show()

#  Managing data with generators
'''
Generators
never in the memory simutanionslly!
'''

filename = 'pb_cm_prodestoquedia_tst_hist_202007291054.csv'
data = pd.read_csv(filename)

def filter_is_large_stock(data):
    "Returns df filtering trips larger than 1000 units"
    is_large_stock = (data.estoque_disponivel > 1000)
    return data.loc[is_large_stock]

#  solving with a list comprehension
chunks_lc = [filter_is_large_stock(chunk) for chunk in pd.read_csv(filename, chunksize=1000)]
# using a generator

chunks_gen = (filter_is_long_trip(chunk) for chunk in pd.read_csv(filename, chunksize=1000))

#  filtering & summing with generators
totalsale = (data.estoque_disponivel.sum() for chunk in chunks)

# sum all sales
sum(totalsale)

# Reading many files

template = 'yellow_tripdata_2015-{:02d}.csv'

filename = (template.format(k) for k in range(1,13)) 
for fname in filename
    print(fname)

# Define the generator: dataframes
dataframes = (pd.read_csv(file) for file in filenames)

# Create the list comprehension: monthly_delayed
monthly_delayed = [pct_delayed(df) for df in dataframes]

# Create the plot
x = range(1,13)
plt.plot(x, monthly_delayed, marker='o', linewidth=0)
plt.ylabel('% Delayed')

#  start with dask
'''
Building a pipeline with delayed

If we use dask.delayed, we don't need to use generators; the dask scheduler will manage memory usage. In this version of the flight delay analysis, you'll compute the total yearly percentage of delayed flights.

Along with pandas, the decorator function delayed has been imported for you from dask, and the following decorated function, which calls pd.read_csv() on a single file, has been created for you:
'''



'''
Your job is to define three decorated functions to complete the pipeline: a function to total the number of flights, a function to count the number of delayed flights, and a function to aggregate the results.
'''

from dask import delayed


@delayed
def read_one(filename):
    return pd.read_csv(filename)

# Define count_flights
@delayed
def count_flights(df):
    return len(df)

# Define count_delayed
@delayed
def count_delayed(df):
    return (df['DEP_DELAY']>0).sum()

# Define pct_delayed
@delayed
def pct_delayed(n_delayed,n_flights):
    return 100 * sum(n_delayed) / sum(n_flights)

# Loop over the provided filenames list and call read_one: df
for file in filenames:
    df = read_one(file)

    # Append to n_delayed and n_flights
    n_delayed.append(count_delayed(df))
    n_flights.append(count_flights(df))


# Call pct_delayed with n_delayed and n_flights: result
result = pct_delayed(n_delayed,n_flights)

# Print the output of result.compute()
print(result.compute())

'''
Notice that no reading and no computation was done until the last line (result.compute()). In all the preceding lines, the functions called returned dask.delayed objects that deferred execution until the invocation of compute().
'''

# Dask arrays

import numpy as np
a = np.random.rand(10000)

print(a.shape, a.dtype)
import dask.array as da
a_dask = da.from_array(a, chunks=len(a) // 4)
print(a_dask.chunks)

# Call da.from_array():  energy_dask
energy_dask = da.from_array(energy, chunks=len(energy) // 4)

# Print energy_dask.chunks
print(energy_dask.chunks)

# Print Dask array average and then NumPy array average
print(energy_dask.mean().compute())
print(energy.mean())

'''
The mean electricity load each 15 minutes is almost 6,100 kWh. As you would expect, the dask.array method mean produces the same value as the numpy.array method mean.
'''

# Import time
import time

# Call da.from_array() with arr: energy_dask4
energy_dask4 = da.from_array(energy, chunks=energy.shape[0]//4)

# Print the time to compute standard deviation
t_start = time.time()
std_4 = energy_dask4.std().compute()
t_end = time.time()
print((t_end - t_start) * 1.0e3)
# output: 33

# Import time
import time

# Call da.from_array() with arr: energy_dask8
energy_dask8 = da.from_array(energy, chunks=energy.shape[0]//8)

# Print the time to compute standard deviation
t_start = time.time()
std_8 = energy_dask8.std().compute()
t_end = time.time()
print((t_end - t_start) * 1.0e3)
# output: 33

'''
Well done! The difference in time is not very perceivable for problems this small.
'''

#  Computing with Multidimensional Arrays
'''
The one-dimensional array load_2001 holds the total electricity load for the state of Texas sampled every 15 minutes for the entire year 2001 (35040 samples in total). The one-dimensional array load_recent holds the corresponding data sampled for each of the years 2013 through 2015 (i.e., 105120 samples consisting of the samples from 2013, 2014, & 2015 in sequence). None of these years are leap years, so each year has 365 days. Observe also that there are 96 intervals of duration 15 minutes in each day.

Your job is to compute the differences of the samples in the years 2013 to 2015 each from the corresponding samples of 2001
'''

'''
Reshape load_recent to three dimensions, with the year on the 1st dimension, the day of the year on the 2nd dimension, and the 15-minute interval of the day on the 3rd dimension.
'''
# Reshape load_recent to three dimensions: load_recent_3d
load_recent_3d = load_recent.reshape((3,365,96))

# Reshape load_recent to three dimensions: load_recent_3d
load_recent_3d = load_recent.reshape((3,365,96))

'''
Now reshape load_2001 to three dimensions, indexed with the year on the leading dimension, the day of the year on the next dimension, and the 15-minute interval of the day on the trailing dimension. Note that you only have 1 year here.
'''

# Reshape load_2001 to three dimensions: load_2001_3d
load_2001_3d = load_2001.reshape((1,365,96))

# Subtract the load in 2001 from the load in 2013 - 2015: diff_3d
diff_3d = load_recent_3d - load_2001_3d

# Print mean value of load_recent_3d
print(load_recent_3d.mean())

# Print maximum of load_recent_3d across 2nd & 3rd dimensions
print(load_recent_3d.max(axis=(1,2)))

# Compute sum along last dimension of load_recent_3d: daily_consumption
daily_consumption = load_recent_3d.sum(axis=-1)

# Print mean of 62nd row of daily_consumption
print(daily_consumption[:, 61].mean())

'''
reading HDF5 file
'''

# Import h5py and dask.array
import h5py
import dask.array as da

# List comprehension to read each file: dsets
dsets = [h5py.File(f)['/tmax'] for f in filenames]

# List comprehension to make dask arrays: monthly
monthly = [da.from_array(d, chunks=(1,444,922)) for d in dsets]

'''
Stacking data & reading climatology
Now that we have a list of Dask arrays, your job is to call da.stack() to make a single Dask array where month and year are in separate dimensions. You'll also read the climatology data set, which is the monthly average max temperature again from 1970 to 2000.
'''

# Stack with the list of dask arrays: by_year
by_year = da.stack(monthly, axis=0)

# Print the shape of the stacked arrays
print(by_year.shape)

# Read the climatology data: climatology
dset = h5py.File('tmax.climate.hdf5')
climatology = da.from_array(dset['/tmax'], chunks=(1,444,922))

# Reshape the climatology data to be compatible with months
climatology = climatology.reshape((1,12,444,922))

# Compute the difference: diff
diff = (by_year - climatology) * 9/5
# Compute the average over last two axes: avg
avg = da.nanmean(diff, axis=(-1,-2)).compute()
# Plot the slices [:,0], [:,7], and [:11] against the x values
x = range(2008,2012)
f, ax = plt.subplots()
ax.plot(x,avg[:,0], label='Jan')
ax.plot(x,avg[:,7], label='Aug')
ax.plot(x,avg[:,11], label='Dec')
ax.axhline(0, color='red')
ax.set_xlabel('Year')
ax.set_ylabel('Difference (degrees Fahrenheit)')
ax.legend(loc=0)
plt.show()

#  Using Dask DataFrames
is_wendy = (transactions['names']= 'Wendy') #dask bool
wendy_amounts = transactions.loc[is_wendy, 'amount']
wendy_amounts

'''
Inspecting a large DataFrame
A Dask DataFrame is provided for you called df. This DataFrame connects to the World Development Indicators data set you worked with earlier.

Your job is to inspect this Dask DataFrame using methods and correctly identify the number of columns, the number of rows, and the number of unique countries from either the Country Name or Country Code columns. You can use methods you are already familiar with such as .describe() and .info(). Remember to also use .compute(), since df is a Dask (and not pandas) DataFrame.

dask documentation:
    https://docs.dask.org/en/latest/dataframe-api.html
'''

# get the num of rows
'''
intersting: is storade as a delayed obj, so you have to compute
'''

df.shape[0].compute()

'''
Building a pipeline of delayed tasks
For this exercise, you'll use a Dask DataFrame to read and process the World Bank's World Development Indicators.

Your job is to filter the DataFrame for the 'East Asia & Pacific' region and measurements of the percent population exposed to toxic air pollution. The output of this effort is a delayed Dask DataFrame; you'll compute the result in the next exercise.

The CSV file 'WDI.csv' has been truncated to reduce execution time.
'''

# Read from 'WDI.csv': df
df = dd.read_csv('WDI.csv')

# Boolean series where 'Indicator Code' is 'EN.ATM.PM25.MC.ZS': toxins
toxins = df['Indicator Code'] == 'EN.ATM.PM25.MC.ZS'
# Boolean series where 'Region' is 'East Asia & Pacific': region
region = df['Region'] == 'East Asia & Pacific'

# Filter the DataFrame using toxins & region: filtered
filtered = df[toxins & region]

# Grouping & aggregating by year
'''
Your job is to use .groupby() to collect all of the individual country values by the 'Year' column and aggregate with the mean() function. You'll then call .compute() to perform the computation in parallel, and finally plot the results.
'''

# Grouby filtered by the 'Year' column: yearly
yearly = filtered.groupby('Year')

# Calculate the mean of yearly: yearly_mean
yearly_mean = yearly.mean()

# Call .compute() to perform the computation: result
result = yearly_mean.compute()

# Plot the 'value' column with .plot.line()
result['value'].plot.line()
plt.ylabel('% pop exposed')
plt.show()

# Timing Dask DataFrame Operations

'''
How big is big data?

Data size M    |    Required hardware
M < 8 GB       |    RAM (single machine)
8GB < M < 10 TB|    hard disk (single machine)
M > 10 TB      |    specialized hardware

2 qestions:
    -   Data fits in RAM?
    -   Data fits on hard disk?

minimal hardware requiriments are defined by than.


pandas or dask?

How big is dataset?
How much RAM available?
How many threads/cores/CPUS available?
Is computation I/O-bound (disk-intensive) or CPU-bound (processor intensive)?

Best case for Dask:
    -   COmputations from Pandas API available in Dask
    -   Problem size close to limits of RAM, fits on disk

'''

'''
Preparing the pipeline
Since pandas and Dask share the same API, we can write functions that work for both libraries. Your job here is to write a function that takes a DataFrame as input, performs Boolean filtering, groupby, and returns the result.

In the next exercise you'll apply this function on Dask and pandas DataFrames and compare the time it takes to complete.
'''

import dask.dataframe as dd

# Define a function with df as input: by_region
def by_region(df):

    # Create the toxins array
    toxins = df['Indicator Code'] == 'EN.ATM.PM25.MC.ZS'

    # Create the y2015 array
    y2015 = df['Year'] == 2015

    # Filter the DataFrame and group by the 'Region' column
    regions = df[y2015 & toxins].groupby('Region')

    # Return the mean of the 'value' column of regions
    return regions['value'].mean()

"Reading & cleaning files"

# Read all .csv files: df
df = dd.read_csv('taxi/*.csv', assume_missing=True)

# Make column 'tip_fraction'
df['tip_fraction'] = df['tip_amount'] / (df['total_amount'] - df['tip_amount'])

# Convert 'tpep_dropoff_datetime' column to datetime objects
df['tpep_dropoff_datetime'] = dd.to_datetime(df['tpep_dropoff_datetime'])

# Construct column 'hour'
df['hour'] = df['tpep_dropoff_datetime'].dt.hour


'''
Your task now is to build a pipeline of computations to compute the hourly average tip fraction for each hour of the day across the entire year of data. You'll have to filter for payments of type 1 (credit card transactions) from the 'payment_type' column, group transactions using the 'hour' column, and finally aggregate the mean from the 'tip_fraction' column.
'''

# Filter rows where payment_type == 1: credit
credit = df.loc[df['payment_type']==1]

# Group by 'hour' column: hourly
hourly = credit.groupby('hour')

# Aggregate mean 'tip_fraction' and print its data type
result = hourly['tip_fraction'].mean()
print(type(result))


'''
Computing & plotting
Now that you've got the entire delayed pipeline prepared it's time compute and plot the result. Matplotlib has been imported for you as plt.
'''
# Perform the computation
tip_frac = result.compute()

# Print the type of tip_frac
print(type(tip_frac))

# Generate a line plot using .plot.line()
tip_frac.plot.line()
plt.ylabel('Tip fraction')
plt.show()



#  Preparing Flight Delay Data

'''

'''
# Define @delayed-function read_flights
@delayed
def read_flights(filename):

    # Read in the DataFrame: df
    df = pd.read_csv(filename, parse_dates=['FL_DATE'])

    # Replace 0s in df['WEATHER_DELAY'] with np.nan
    df['WEATHER_DELAY'] = df['WEATHER_DELAY'].replace(0, np.nan)

    # Return df
    return df

# Loop over filenames with index filename
for filename in filenames:
    # Apply read_flights to filename; append to dataframes
    dataframes.append(read_flights(filename))

# Compute flight delays: flight_delays
flight_delays = dd.from_delayed(dataframes)

# Print average of 'WEATHER_DELAY' column of flight_delays
print(flight_delays['WEATHER_DELAY'].mean().compute())

# Define @delayed-function read_weather with input filename
@delayed
def read_weather(filename):
    # Read in filename: df
    df = pd.read_csv(filename, parse_dates=['Date'])

    # Clean 'PrecipitationIn'
    df['PrecipitationIn'] = pd.to_numeric(df['PrecipitationIn'], errors='coerce')

    # Create the 'Airport' column
    df['Airport'] = filename.split('.')[0]

    # Return df
    return df

'''
Your job now is to construct a Dask DataFrame using the function from the previous exercise.
'''
# Loop over filenames with filename
for filename in filenames:
    # Invoke read_weather on filename; append resultt to weather_dfs
    weather_dfs.append(read_weather(filename))

# Call dd.from_delayed() with weather_dfs: weather
weather = dd.from_delayed(weather_dfs)

# Print result of weather.nlargest(1, 'Max TemperatureF')
print(weather.nlargest(1, 'Max TemperatureF').compute())

# Make cleaned Boolean Series from weather['Events']: is_snowy
is_snowy = weather['Events'].str.contains('Snow').fillna(False)

# Create filtered DataFrame with weather.loc & is_snowy: got_snow
got_snow = weather.loc[is_snowy]

# Groupby 'Airport' column; select 'PrecipitationIn'; aggregate sum(): result
result = got_snow.groupby('Airport')['PrecipitationIn'].sum()

# Compute & print the value of result
print(result.compute())

# Merging & Persisting DataFrames

# Print time in milliseconds to compute percent_delayed on weather_delays
t_start = time.time()
print(percent_delayed(weather_delays).compute())
t_end = time.time()
print((t_end-t_start)*1000)

# Call weather_delays.persist(): persisted_weather_delays
persisted_weather_delays = weather_delays.persist()

# Print time in milliseconds to compute percent_delayed on persisted_weather_delays
t_start = time.time()
print(percent_delayed(persisted_weather_delays).compute())
t_end = time.time()
print((t_end-t_start)*1000)

# Group persisted_weather_delays by 'Events': by_event
by_event = persisted_weather_delays.groupby('Events')

# Count 'by_event['WEATHER_DELAY'] column & divide by total number of delayed flights
pct_delayed = by_event['WEATHER_DELAY'].count() / persisted_weather_delays['WEATHER_DELAY'].count() * 100

# Compute & print five largest values of pct_delayed
print(pct_delayed.nlargest(5).compute())

# Calculate mean of by_event['WEATHER_DELAY'] column & return the 5 largest entries: avg_delay_time
avg_delay_time = by_event['WEATHER_DELAY'].mean().nlargest(5)

# Compute & print avg_delay_time
print(avg_delay_time.compute())

# deploying Dask
# integration with other python libraries
# dynamic task scheduling and data management



# Building Dask Bags & Globbing
'''
 
'''
# Glob filenames matching 'sotu/*.txt' and sort
filenames = glob.glob('sotu/*.txt')
filenames = sorted(filenames)

# Load filenames as Dask bag with db.read_text(): speeches
speeches = db.read_text(filenames)

# Print number of speeches with .count()
print(speeches.count().compute())

# Call .take(1): one_element
one_element = speeches.take(1)

# Extract first element of one_element: first_speech
first_speech = one_element[0]

# Print type of first_speech and first 60 characters
print(type(first_speech))
print(first_speech[:60])

# functional programming with dask
import dask.bag as db
presidents.str.upper()

# Call .str.split(' ') from speeches and assign it to by_word
by_word = speeches.str.split(' ')

# Map the len function over by_word and compute its mean
n_words = by_word.map(len)
avg_words = n_words.mean()

# Print the type of avg_words and value of avg_words.compute()
print(type(avg_words))
print(avg_words.compute())

# Convert speeches to lower case: lower
lower = speeches.str.lower()

# Filter lower for the presence of 'health care': health
health = lower.filter(lambda s:'health care' in s)

# Count the number of entries : n_health
n_health = health.count()

# Compute and print the value of n_health
print(n_health.compute())

# Call db.read_text with congress/bills*.json: bills_text
bills_text = db.read_text('congress/bills*.json')

# Map the json.loads function over all elements: bills_dicts
bills_dicts = bills_text.map(json.loads)

# Extract the first element with .take(1) and index to the first position: first_bill
first_bill = bills_dicts.take(1)[0]

# Print the keys of first_bill
print(first_bill.keys())

# Define a function lifespan that takes a dictionary d as input
def lifespan(d):
    # Convert to datetime
    current = pd.to_datetime(d['current_status_date'])
    intro = pd.to_datetime(d['introduced_date'])

    # Return the number of days
    return (current - intro).days

# Filter bills_dicts: days
days = bills_dicts.filter(lambda s:s['current_status']=='enacted_signed').map(lifespan)

# Print the mean value of the days Bag
print(days.mean().compute())
