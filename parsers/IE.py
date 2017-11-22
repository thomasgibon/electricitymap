# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 14:54:54 2017

@author: t.gibon@gmail.com
"""

# The arrow library is used to handle datetimes
import arrow
# The request library is used to fetch content through HTTP
import requests
# Used to parse date info into standard datetimes, arrow should do that too?
from datetime import datetime


FUEL_MAP = {
        'FUEL_COAL':'coal',
        'FUEL_EWIC':'GB->IE', # Imports seem to be accounted in the fuel mix
        'FUEL_GAS' :'gas',
        'FUEL_OTHER_FOSSIL':'unknown',  # The Fuel Mix Graph is broken down into
                                        # Gas, Coal, Renewables, Oil, Net import
                                        # and Other. The “Other” category includes
                                        # Peat, Combined Heat and Power (CHP),
                                        # Aggregated Generating Units (AGUs), Demand Side
                                        # Units (DSUs), Distillate and Waste.
        'FUEL_RENEW':'wind'} # Renewable in Ireland is almost only wind

DATE_FORMAT = '%d-%b-%Y %H:%M:%S'


def fetch_production(country_code='IE', session=None):
    """Requests the last known production mix (in MW) of a given country
    Arguments:
    country_code (optional) -- used in case a parser is able to fetch multiple countries
    session (optional)      -- request session passed in order to re-use an existing session
    Return:
    A dictionary in the form:
    {
      'countryCode': 'IE',
      'datetime': '2017-01-01T00:00:00Z',
      'production': {
          'biomass': 0.0,
          'coal': 0.0,
          'gas': 0.0,
          'hydro': 0.0,
          'nuclear': null,
          'oil': 0.0,
          'solar': 0.0,
          'wind': 0.0,
          'geothermal': 0.0,
          'unknown': 0.0
      },
      'storage': {
          'hydro': -10.0,
      },
      'source': 'mysource.com'
    }
    """
    
    data = {
        'countryCode': country_code,
        'production': {},
        'storage': {},
        'source': 'smartgriddashboard.eirgrid.com'
    }
    
    # Request parameters
    #
    # Region codes
    # - Republic of Ireland: ROI
    # - Northern Ireland:    NI
    # - Entire island:       ALL
    
    if country_code == 'IE':
        region = 'ROI'
    else:
        region = 'All' # Not sure if the parameters have the same names...
    
    date   = datetime.strftime(datetime.now(), '%d-%b-%Y')
    
    r   = session or requests.session()
    url = 'http://smartgriddashboard.eirgrid.com/DashboardService.svc/data?datefrom={}+00%3A00&dateto={}+23%3A59'.format(date, date)
    
    # Power generation, total, every 15 minutes, in MW
    params = {'area':'generationactual',
              'region'  : region}
    
    response = r.get(url, params = params, timeout = 5)
    obj      = response.json()
    
    data['datetime']   = datetime.strptime(obj['LastUpdated'], DATE_FORMAT).isoformat()
    total_generation = [row['Value'] for row in obj['Rows']
                            if row['EffectiveTime'] == obj['LastUpdated']][0]
    
    # Fuel mix, every 24 hours, in %
    params = {'area'  : 'fuelmix',
              'region': region}
    
    response = r.get(url, params = params, timeout = 5)
    obj = response.json()
    
    # Break down the total generation by fuel mix, in MW
    calculated_generation = [(row['FieldName'],row['Value']/100*total_generation)
                             for row in obj['Rows'] if row['EffectiveTime'] == obj['LastUpdated']]
    
    for source in calculated_generation:
        # All production values should be >= 0
        if source[0] != 'FUEL_EWIC':
            data['production'][FUEL_MAP[source[0]]] = source[1] # Should be a floating point value

#    for item in obj['storage']:
#        # Positive storage means energy is stored
#        # Negative storage means energy is generated from the storage system
#        data['storage'][item['key']] = item['value'] # Should be a floating point value

    # Parse the datetime and return a python datetime object
    data['datetime'] = arrow.get(data['datetime']).datetime

    return data



def fetch_price(country_code='IE', session=None):
    """Requests the last known power price of a given country
    Arguments:
    country_code (optional) -- used in case a parser is able to fetch multiple countries
    session (optional)      -- request session passed in order to re-use an existing session
    Return:
    A dictionary in the form:
    {
      'countryCode': 'FR',
      'currency': EUR,
      'datetime': '2017-01-01T00:00:00Z',
      'price': 0.0,
      'source': 'mysource.com'
    }
    """
    
    if country_code == 'IE':
        region = 'ROI'
    else:
        region = 'All' # Not sure if the parameters have the same names...
        
    date   = datetime.strftime(datetime.now(), '%d-%b-%Y')
    
    r = session or requests.session()
    
    # Market price stats
    url = 'http://smartgriddashboard.eirgrid.com/DashboardService.svc/stats?datefrom={}+00%3A00&dateto={}+23%3A59'.format(date, date)
    params = {'area'  : 'MarketPriceStats',
              'region': region}
    response = r.get(url, params = params)
    obj = response.json()
    latest = [row for row in obj['Rows'] if row['FieldName'] == 'LATEST_MARKET_PRICE'][0]
    
    # Market data
    url = 'http://smartgriddashboard.eirgrid.com/DashboardService.svc/marketdata?datefrom={}+00%3A00&dateto={}+23%3A59'.format(date, date)
    params = {'area'  : 'marketdata',
              'region': region}
    
    response = r.get(url, params = params)
    obj = response.json()
    
    EurPrice = [row['EurPrice'] for row in obj['Rows'] if row['EffectiveTime'] == latest['EffectiveTime']][0]
            
    data = {
        'countryCode': country_code,
        'currency': 'EUR',
        'price': EurPrice,
        'source': 'smartgriddashboard.eirgrid.com',
    }
    
    data['datetime']   = datetime.strptime(latest['EffectiveTime'], '%d-%b-%Y %H:%M:%S').isoformat()
    
    # Parse the datetime and return a python datetime object
    data['datetime'] = arrow.get(data['datetime']).datetime

    return data


def fetch_exchange(country_code1='GB', country_code2='IE', session=None):
    """Requests the last known power exchange (in MW) between two countries
    Arguments:
    country_code (optional) -- used in case a parser is able to fetch multiple countries
    session (optional)      -- request session passed in order to re-use an existing session
    Return:
    A dictionary in the form:
    {
      'sortedCountryCodes': 'DK->NO',
      'datetime': '2017-01-01T00:00:00Z',
      'netFlow': 0.0,
      'source': 'mysource.com'
    }
    """

    if country_code2 == 'IE':
        region = 'ROI'
    else:
        region = 'All' # Not sure if the parameters have the same names...
        
    date   = datetime.strftime(datetime.now(), '%d-%b-%Y')
    
    r = session or requests.session()
    url = 'http://smartgriddashboard.eirgrid.com/DashboardService.svc/data?datefrom={}+00%3A00&dateto={}+23%3A59'.format(date, date)
    params = {'area'  : 'interconnection',
              'region': region}
    
    response = r.get(url, params = params)
    obj = response.json()
    
    latest = [(row['EffectiveTime'],row['Value']) for row in obj['Rows'] if row['Value']][-1]
    
    data = {
        'sortedCountryCodes': '->'.join(sorted([country_code1, country_code2])),
        'source': 'smartgriddashboard.eirgrid.com',
        'datetime': datetime.strptime(latest[0], '%d-%b-%Y %H:%M:%S').isoformat()
    }
    
    # Country codes are sorted in order to enable easier indexing in the database
    sorted_country_codes = sorted([country_code1, country_code2])
    # Here we assume that the net flow returned by the api is the flow from 
    # country1 to country2. A positive flow indicates an export from country1
    # to country2. A negative flow indicates an import.
    #
    # However, the EirGrid data does exactly the opposite, a negative flow is
    # an export from Ireland to the UK
    netFlow = latest[1]
    # The net flow to be reported should be from the first country to the second
    # (sorted alphabetically). This is NOT necessarily the same direction as the flow
    # from country1 to country2
    data['netFlow'] = netFlow if country_code1 == sorted_country_codes[0] else -1 * netFlow

    # Parse the datetime and return a python datetime object
    data['datetime'] = arrow.get(data['datetime']).datetime

    return data

if __name__ == '__main__':
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print('fetch_production() ->')
    print(fetch_production())
    print('fetch_price() ->')
    print(fetch_price())
    print('fetch_exchange(GB, IE) ->')
    print(fetch_exchange('GB', 'IE'))
