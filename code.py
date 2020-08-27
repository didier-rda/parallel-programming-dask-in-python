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
def filter_is_long_trip(data):
    "Returns df filtering trips longer than 20 mins"
    is_long_trip = (data.trip_time_in_secs > 1200)
    return data.loc[is_long_trip]

chunks = [filter_is_long_trip(chunk) for chunk in pd.read_csv(filename, chunksize=1000)]

#  filtering & summing with generators

chunks = (filter_is_long_trip(chunk) for chunk in pd.read_csv(filename, chunksize=1000))

'''
never in the memory simutanionslly!

'''
data = pd.read_csv('pb_cm_prodestoquedia_tst_hist_202007291054.csv')

def filter_is_large_stock(data):
    "Returns df filtering trips larger than 1000 units"
    is_large_stock = (data.estoque_disponivel > 1000)
    return data.loc[is_large_stock]

chunks = [filter_is_long_trip(chunk) for chunk in pd.read_csv(filename, chunksize=1000)]

#  filtering & summing with generators

chunks = (filter_is_long_trip(chunk) for chunk in pd.read_csv(filename, chunksize=1000))

'''
never in the memory simutanionslly!

'''

