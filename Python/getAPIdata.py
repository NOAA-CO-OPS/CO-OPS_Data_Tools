## Data retrieval functions for CO-OPS Python Module

import datetime as dt
import urllib as ul
import csv as csv

def getData(stationID,
            dateRange = [dt.datetime.now() - dt.timedelta(days=31),
                         dt.datetime.now()],
            product   = 'water_level',
            datum     = 'STND',
            units     = 'metric',
            timeZone  = 'gmt',
            interval  = [],
            binNum    = []):

    # This function will get data via the CO-OPS data API and return a Python Dictionary
    # containing the downloaded data and metadata.  Input are as follows:
    # stationID - a string containing the digit water level station ID or the current meter designation (Required)
    # dateRange - a datetime list containing a starting and ending date for data retrieval
    #                     default is the previous 31 days from the time when the function is called
    # product   - the data product to be retrieved.  The options are:
    #   water_level             - Preliminary or verifed water levels, depending on availability
    #   air_temperature         - Air temperature as measured at the station
    #   water_temperature       - Water temperature as measured at the station
    #   wind                    - Wind speed, direction, and gusts as measured at the station
    #   air_pressure            - Barometric pressure as measured at the station
    #   air_gap                 - Air Gap (distance between a bridge and the water's surface) at the station
    #   conductivity            - The water's conductivity as measered at the station
    #   visibility              - visibility from the station's visibility sensor. A measure of atmopheric clarity
    #   humidity                - relative humidity as mesred at the station
    #   salinity                - Salinity and specific gravity data for the station
    #   hourly_height           - Verified hourly height water level data for the station
    #   high_low                - Verified high/low water level data for the station
    #   daily_mean              - Verified daily mean water level data for the station
    #   one_minute_water_level  - One minute water level data for the station
    #   predictions             - 6 minute predicted water level data for the station
    #   datums                  - datums data for the stations
    #   currents                - Currents data for the currents stations
    # datum     - the datum bias for water level data.  Options are:
    #   MHHW    - Mean higher high water
    #   MHW     - Mean high water
    #   MSL     - Mean sea level
    #   MTL     - Mean tide level
    #   MLW     - Mean low water
    #   MLLW    - Mean lower low water
    #   NAVD    - North American Vertical Datum
    #   STND    - Station Datum (Default)
    #   IGLD    - International Great Lakes Datum
    #   CRD     - Columbia River Datum
    # units     - the units to return the data. Options are
    #   metric  - Metric (Celcius, meters) units
    #   english - English (farenheit, feet) units
    # timeZone  - The time zone to return the data.  Options are:
    #   gmt     - Greenwich mean time (default)
    #   lst     - local standard time.  Local to the station.
    #   lst_ldt - local standard/local daylight time. Local to the station.
    # interval  - the interval for which Meteorlogical data is returned. The defaults is 6 minute intervals
    #   and there is no need to specify it.  Only specify 'interval' for :
    #   h       - hourly Met data and predictions data
    #   hilo    - High/Low tide predictions for subordinate stations.
    # binNum    - the bin number for the spcified currenstation.  If not specified the returned data will correspond to the
    #   designated real-time bin. Example (bin='3')
    #
    # LL - 2/5/2018

    ## Parse the dates and develop the starting and ending dates to call
    
    # define the time steps of the data
    if (product == 'one_minute_water_level'):
        step = dt.timedelta(days = 5)
    elif (product == 'hourly_height') or (product == 'high_low'):
        step = dt.timedelta(days = 365)
    elif (product == 'predictions') or (product == 'daily_mean') or (product == 'monthly_mean'):
        step = dt.timedelta(days = 10 * 365)
    else:
        step = dt.timedelta(days=31)

    # create the startDate list.  This will be the information used in calling the
    # API recursively.  The end date of the request will be startDate + step - dt.timedelta(minutes=6)
    startDate = [dateRange[0]]

    while (startDate[-1] < dateRange[1]):
        startDate.append(startDate[-1] + step)

    if startDate[-1]>dateRange[1]:
        startDate.remove(startDate[-1])

    # The errorsFound variable will be returned as a list of errors, if any, found
    # during the calling of the URL for the data
    errorsFound = ['No errors were found']
    
    
    # Define if this the first time the URL is being called.  If so, header information
    # will be used to make the output dictionary.  If not, then the output dictionary will
    # be populated with the information from the URL
    firstTime  = 'yes'
    outputDict = dict()
    temp       = dict()

    # URL information
    server = 'https://tidesandcurrents.noaa.gov/api/datagetter'
    
    # call the API recursivly
    for d in startDate:
        # create endDate to call and check if it is greater than the requested end date
        d2 = d + step - dt.timedelta(minutes=6)
        if d2 > dateRange[1]:
            d2 = dateRange[1]

        urlRequest = ( server   +
                    '?begin_date='  + d.strftime('%Y%m%d %H:%M')    +
                    '&end_date='    + d2.strftime('%Y%m%d %H:%M')   +
                    '&station='     + stationID                     +
                    '&product='     + product                       +
                    '&datum='       + datum                         +
                    '&units='       + units                         +
                    '&time_zone='   + timeZone                      +
                    '&application=OD_python&format=csv' )
        
        # Append additional information to the url request if needed.
        if (product=='currents') and (len(binNum)):    
            urlRequest = ( urlRequest  + '&bin=' + binNum)
            
        elif len(interval) > 0:
            urlRequest = ( urlRequest + '&interval=' + interval )

        # Read the url response
        urlResponse = ul.urlopen(urlRequest)
        urlResponse = urlResponse.read()

        # read the csv output
        # get the file headers information
        headers  = urlResponse.split('\n')[0]
        eMessage = urlResponse.split('\n')[1]
        

        # check for errors
        if ('Error' in eMessage):
            errorsFound.append(eMessage)
        else:
            # no errors found
            if firstTime == 'yes':
                # first time calling the url, make the dictionary fields
                for h in headers.split(','):
                    outputDict[h.strip()] = []
                firstTime = 'no'
            
            for line in urlResponse.split('\n')[1:-1]:
                # populate the output dictionary
                for i in range(0,len(headers.split(','))):
                    v = line.split(',')[i]
                    h = headers.split(',')[i].strip()
                        
                    try:
                        temp[h] = float(v)
                    except:
                        try:
                            temp[h] = dt.datetime.strptime(v,'%Y-%m-%d %H:%M')
                        except:
                            temp[h] = v.strip()
                    outputDict[h].append(temp[h])

    # if errors were found, remove the 'no errors were found' part of the message
    if len(errorsFound)>1:
        errorsFound.remove(errorsFound[0])

    # If the request product is datum information, then clean up the output dictionary
    if product == 'datums':
        temp = dict()
        dCount = 0
        for d in outputDict['Datum']:
            temp[d] = outputDict['Value'][dCount]
            dCount += 1
        outputDict = temp

        
    return outputDict, errorsFound
