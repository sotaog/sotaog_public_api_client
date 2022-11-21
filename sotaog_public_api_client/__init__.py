import csv
import logging
import os

import requests

logger = logging.getLogger('sotaog_public_api_client')
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))


class Client_Exception(Exception):
  pass


class Client():
  def __init__(self, url, client_id, client_secret, customer_id = None):
    self.session = requests.Session()
    self.url = url.rstrip('/')
    self.customer_id = customer_id
    logger.info('Initializing Sotaog API client for {}'.format(url))
    logger.debug('Authenticating to API: {}'.format(url))
    data = {
        'grant_type': 'client_credentials'
    }
    result = self.session.post('{}/v1/authenticate'.format(self.url), data=data, auth=(client_id, client_secret))
    if result.status_code == 200:
      self.token = result.json()['access_token']
      logger.debug('Token: {}'.format(self.token))
    else:
      raise Client_Exception('Unable to authenticate to API')

  def _get_headers(self):
    headers = {
        'authorization': 'Bearer {}'.format(self.token)
    }
    if self.customer_id:
      headers['x-sotaog-customer-id'] = self.customer_id
    return headers

  def get_alarm_services(self):
    logger.debug('Getting alarm services')
    headers = self._get_headers()
    result = self.session.get('{}/v1/alarm-services'.format(self.url), headers=headers)
    if result.status_code == 200:
      alarm_services = result.json()
      logger.debug('Alarm Services: {}'.format(alarm_services))
      return alarm_services
    else:
      raise Client_Exception('Unable to retrieve alarm services')

  def get_alarm_service(self, alarm_service_id):
    logger.debug('Getting alarm service {}'.format(alarm_service_id))
    headers = self._get_headers()
    url = '{}/v1/alarm-services/{}'.format(self.url, alarm_service_id)
    result = self.session.get(url, headers=headers)
    if result.status_code == 200:
      alarm_service = result.json()
      logger.debug('Alarm Service: {}'.format(alarm_service))
      return alarm_service
    else:
      raise Client_Exception('Unable to retrieve alarm service {}'.format(alarm_service_id))

  def get_alarms(self):
    logger.debug('Getting alarms')
    headers = self._get_headers()
    result = self.session.get('{}/v1/alarms'.format(self.url), headers=headers)
    if result.status_code == 200:
      alarms = result.json()
      logger.debug('Alarms: {}'.format(alarms))
      return alarms
    else:
      raise Client_Exception('Unable to retrieve alarms')
    
  def get_custom_alarms(self):
    logger.debug('Getting alarms')
    headers = self._get_headers()
    result = self.session.get('{}/v1/custom-alarms'.format(self.url), headers=headers)
    if result.status_code == 200:
      alarms = result.json()
      logger.debug('Alarms: {}'.format(alarms))
      return alarms
    else:
      raise Client_Exception('Unable to retrieve alarms')
  
  def get_custom_alarm(self,alarms_id):
    logger.debug('Getting alarms')
    headers = self._get_headers()
    result = self.session.get('{}/v1/custom-alarms/{}'.format(self.url, alarms_id), headers=headers)
    if result.status_code == 200:
      alarms = result.json()
      logger.debug('Alarms: {}'.format(alarms))
      return alarms
    else:
      raise Client_Exception('Unable to retrieve alarms')
  
  def get_alarm_incidents(self,alarm_id, well_id, alarm_status):
    logger.debug('Getting alarms')
    headers = self._get_headers()
    url = '{}/v1/custom-alarms-incidents?alarm_id={}&well_id={}&alarm_status={}'.format(self.url,alarm_id,well_id,alarm_status)   
    result = self.session.get(url, headers=headers)
    if result.status_code == 200:
      alarms = result.json()
      logger.debug('Alarms: {}'.format(alarms))
      return alarms
    else:
      raise Client_Exception('Unable to retrieve alarms')
  
  def post_custom_alarm_incidents(self, incidents):
    logger.debug('Creating Alarm Incidents {}'.format(incidents))
    headers = self._get_headers()
    result = self.session.put('{}/v1/custom-alarms-incidents'.format(self.url), headers=headers, json=incidents)
    if result.status_code == 201:
      created = result.json()
      logger.debug('Alarms Incidents: {}'.format(created))
      return created
    else:
      raise Client_Exception('Unable to create Alarm Incidents')

  def get_alarm(self, asset_id, datatype = None):
    logger.debug('Getting alarms for {}'.format(asset_id))
    headers = self._get_headers()
    url = '{}/v1/alarms/{}'.format(self.url, asset_id)
    if datatype:
      url += '/{}'.format(datatype)
    result = self.session.get(url, headers=headers)
    if result.status_code == 200:
      alarm = result.json()
      logger.debug('Alarm: {}'.format(alarm))
      return alarm
    else:
      raise Client_Exception('Unable to retrieve alarms for {}'.format(asset_id))

  def get_facilities(self):
    logger.debug('Getting facilities')
    headers = self._get_headers()
    result = self.session.get('{}/v1/facilities'.format(self.url), headers=headers)
    if result.status_code == 200:
      facilities = result.json()
      logger.debug('Facilities: {}'.format(facilities))
      return facilities
    else:
      raise Client_Exception('Unable to retrieve facilities')

  def get_facility(self, facility_id):
    logger.debug('Getting facility: {}'.format(facility_id))
    headers = self._get_headers()
    result = self.session.get('{}/v1/facilities/{}'.format(self.url, facility_id), headers=headers)
    if result.status_code == 200:
      facility = result.json()
      logger.debug('Facility: {}'.format(facility))
      return facility
    else:
      raise Client_Exception('Unable to retrieve facility {}'.format(facility_id))

  def get_facility_config(self, facility_id):
    logger.debug('Getting config for {}'.format(facility_id))
    headers = self._get_headers()
    result = self.session.get('{}/v1/facilities/{}/config'.format(self.url, facility_id), headers=headers)

    if result.status_code == 200:
      config = result.json()
      logger.debug('config: {}'.format(config))
      return config
    else:
      raise Client_Exception('Unable to retrieve config')

  def get_asset(self, asset_id, type = 'assets'):
    logger.debug('Getting asset {} of type: {}'.format(asset_id, type))
    headers = self._get_headers()
    result = self.session.get('{}/v1/{}/{}'.format(self.url, type, asset_id), headers=headers)
    if result.status_code == 200:
      asset = result.json()
      logger.debug('Asset: {}'.format(asset))
      return asset
    else:
      raise Client_Exception('Unable to retrieve asset {} of type {}'.format(asset_id, type))

  def get_assets(self, type = 'assets', facility = None, asset_type = None):
    logger.debug('Getting assets of type: {}'.format(type))
    headers = self._get_headers()
    result = self.session.get('{}/v1/{}'.format(self.url, type), headers=headers)
    if result.status_code == 200:
      assets = result.json()
      if facility:
        assets = [asset for asset in assets if 'facility' in asset and asset['facility'] == facility]
      if asset_type:
        assets = [asset for asset in assets if 'asset_type' in asset and asset['asset_type'] == asset_type]
      logger.debug('Assets: {}'.format(assets))
      return assets
    else:
      raise Client_Exception('Unable to retrieve assets of type {}'.format(asset_type))

  def get_asset_type(self, asset_type_id):
    logger.debug('Getting asset type {}'.format(asset_type_id))
    headers = self._get_headers()
    result = self.session.get('{}/v1/asset-types/{}'.format(self.url, asset_type_id), headers=headers)
    if result.status_code == 200:
      asset_type = result.json()
      logger.debug('Asset Type: {}'.format(asset_type))
      return asset_type
    else:
      raise Client_Exception('Unable to retrieve asset {}'.format(asset_type_id))

  def get_asset_types(self):
    logger.debug('Getting asset types')
    headers = self._get_headers()
    result = self.session.get('{}/v1/asset-types'.format(self.url), headers=headers)
    if result.status_code == 200:
      asset_types = result.json()
      logger.debug('Asset types: {}'.format(asset_types))
      return asset_types
    else:
      raise Client_Exception('Unable to get asset types')

  def get_customers(self):
    logger.debug('Getting customers')
    headers = self._get_headers()
    result = self.session.get('{}/v1/customers'.format(self.url), headers=headers)
    if result.status_code == 200:
      customers = result.json()
      logger.debug('Customers: {}'.format(customers))
      return customers
    else:
      raise Client_Exception('Unable to get customers')

  def get_customer(self, customer_id):
    logger.debug('Getting customer {}'.format(customer_id))
    headers = self._get_headers()
    result = self.session.get('{}/v1/customers/{}'.format(self.url, customer_id), headers=headers)
    if result.status_code == 200:
      customer = result.json()
      logger.debug('Customer: {}'.format(customer))
      return customer
    else:
      raise Client_Exception('Unable to get customer {}'.format(customer_id))

  def get_datatypes(self, group_by='asset'):
    logger.debug('Getting datatypes')
    headers = self._get_headers()
    params = {}
    if group_by:
      params['group_by'] = group_by
    result = self.session.get('{}/v1/datatypes'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      datatypes = result.json()
      logger.debug('Datatypes: {}'.format(datatypes))
      return datatypes
    else:
      raise Client_Exception('Unable to get datatypes')

  def get_datatype(self, datatype_id):
    logger.debug('Getting datatype {}'.format(datatype_id))
    headers = self._get_headers()
    params = {'group_by': 'asset'}
    result = self.session.get('{}/v1/datatypes/{}'.format(self.url, datatype_id), headers=headers, params=params)
    if result.status_code == 200:
        datatype = result.json()
        logger.debug('Datatype: {}'.format(datatype))
        return datatype
    else:
        raise Client_Exception('Unable to get datatype {}'.format(datatype_id))

  def get_datapoints(self, asset_datatypes, start_ts = None, end_ts = None, sort = 'desc', limit = 100):
    logger.debug('Getting datapoints for asset_datatypes: {}'.format(asset_datatypes))
    headers = self._get_headers()
    body = {
        'asset_datatypes': asset_datatypes
    }
    if start_ts:
      body['start_ts'] = start_ts
    if end_ts:
      body['end_ts'] = end_ts
    if sort:
      body['sort'] = sort
    if limit:
      body['limit'] = limit
    result = self.session.post('{}/v1/datapoints'.format(self.url), headers=headers, json=body)
    if result.status_code == 200:
      datapoints = result.json()
      logger.debug('Datapoints: {}'.format(datapoints))
      return datapoints
    else:
      logger.debug(result.json())
      raise Client_Exception('Unable to get datapoints')

  def get_oil_gas_price(self, start_date = None, end_date = None):
    logger.debug('Getting prices')
    headers = self._get_headers()
    params = {}
    if start_date:
      params['start_date'] = start_date
    if end_date:
      params['end_date'] = end_date
    result = self.session.get('{}/v1/financials/oil-gas-price'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      prices = result.json()
      logger.debug('Oil Gas Prices: {}'.format(prices))
      return prices
    else:
      raise Client_Exception('Unable to retrieve Oil Gas prices')

  def get_asset_datapoints(self, asset_id, datatypes = [], start_ts = None, end_ts = None, sort = 'desc', limit = 100):
    logger.debug('Getting datapoints for asset: {}'.format(asset_id))
    headers = self._get_headers()
    params = {}
    if datatypes:
      params['datatypes'] = datatypes
    if start_ts:
      params['start_ts'] = start_ts
    if end_ts:
      params['end_ts'] = end_ts
    if sort:
      params['sort'] = sort
    if limit:
      params['limit'] = limit
    result = self.session.get('{}/v1/datapoints/{}'.format(self.url, asset_id), headers=headers, params=params)
    if result.status_code == 200:
      datapoints = result.json()
      logger.debug('Datapoints: {}'.format(datapoints))
      return datapoints
    else:
      raise Client_Exception('Unable to get datapoints')

  def get_swd_networks(self, facility = None):
    logger.debug('Getting SWD networks')
    headers = self._get_headers()
    result = self.session.get('{}/v1/swd-networks'.format(self.url), headers=headers)
    if result.status_code == 200:
      swd_networks = result.json()
      logger.debug('SWD Networks: {}'.format(swd_networks))
      if facility:
        swd_networks = [swd_network for swd_network in swd_networks if facility in swd_network['facilities']]
      logger.debug('SWD Networks: {}'.format(swd_networks))
      return swd_networks
    else:
      raise Client_Exception('Unable to retrieve SWD networks')

  def get_truck_tickets(self, facility = None, type = None, start_ts = None, end_ts = None):
    logger.debug('Getting truck tickets')
    headers = self._get_headers()
    params = {}
    if start_ts:
      params['start_ts'] = start_ts
    if end_ts:
      params['end_ts'] = end_ts
    if type:
      params['type'] = type
    if facility:
      params['facility'] = facility
    result = self.session.get('{}/v1/truck-tickets'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      truck_tickets = result.json()
      logger.debug('Truck tickets: {}'.format(truck_tickets))
      return truck_tickets
    else:
      raise Client_Exception('Unable to retrieve truck tickets')

  def get_auto_truck_tickets(self, facility = None, type = None, start_ts = None, end_ts = None):
    logger.debug('Getting auto truck tickets')
    headers = self._get_headers()
    params = {}
    if start_ts:
      params['start_ts'] = start_ts
    if end_ts:
      params['end_ts'] = end_ts
    if type:
      params['type'] = type
    if facility:
      params['facility'] = facility
    result = self.session.get('{}/v1/auto-truck-tickets'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      truck_tickets = result.json()
      logger.debug('Auto Truck tickets: {}'.format(truck_tickets))
      return truck_tickets
    else:
      raise Client_Exception('Unable to retrieve truck tickets')

  def post_truck_ticket(self, truck_ticket):
    logger.debug('Creating truck ticket {}'.format(truck_ticket))
    headers = self._get_headers()
    result = self.session.post('{}/v1/truck-tickets'.format(self.url), headers=headers, json=truck_ticket)
    if result.status_code == 201:
      created_ticket = result.json()
      logger.debug('Truck ticket: {}'.format(created_ticket))
      return created_ticket
    else:
      raise Client_Exception('Unable to create truck ticket')

  def post_auto_truck_ticket(self, truck_ticket):
    logger.debug('Creating auto truck ticket {}'.format(truck_ticket))
    headers = self._get_headers()
    result = self.session.post('{}/v1/auto-truck-tickets'.format(self.url), headers=headers, json=truck_ticket)
    if result.status_code == 201:
      created_ticket = result.json()
      logger.debug('Auto Truck ticket: {}'.format(created_ticket))
      return created_ticket
    else:
      raise Client_Exception('Unable to create auto truck ticket')

  def put_truck_ticket(self, truck_ticket_id, timestamp,  truck_ticket):
    logger.debug('Putting truck_ticket for {}'.format(truck_ticket_id))
    headers = self._get_headers()
    result = self.session.post('{}/v1/truck-tickets/{}/{}'.format(self.url, truck_ticket_id, timestamp), headers=headers, json=truck_ticket)
    if result.status_code != 201 and result.status_code != 200:
      logger.exception(result.json())
      raise Client_Exception('Unable to update truck-ticket')

  def put_truck_ticket_image(self, truck_ticket_id, timestamp, image, content_type):
    logger.debug('Creating truck ticket image size {}, content_type {}'.format(len(image), content_type))
    headers = self._get_headers()
    headers['content-type'] = content_type
    result = self.session.put('{}/v1/truck-tickets/{}/{}/image'.format(self.url, truck_ticket_id, timestamp), headers=headers, data=image)
    if result.status_code != 204:
      logger.exception(result.json())
      raise Client_Exception('Unable to create truck ticket image')

  def put_alarm(self, asset_id, datatype, alarm):
    logger.debug('Creating alarm for {} {}'.format(asset_id, datatype))
    headers = self._get_headers()
    result = self.session.put('{}/v1/alarms/{}/{}'.format(self.url, asset_id, datatype), headers=headers, json=alarm)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Exception('Unable to create alarm')

  def post_datapoints(self, asset_id, datapoints):
    logger.debug('Posting datapoints')
    headers = self._get_headers()
    result = self.session.post('{}/v1/datapoints/{}'.format(self.url, asset_id), headers=headers, json=datapoints)
    if result.status_code != 202:
      logger.exception(result.json())
      raise Exception('Unable to post datapoints')

  def batch_put_well_production(self, production):
    logger.debug('Creating well production for {}'.format(production))
    headers = self._get_headers()
    result = self.session.put('{}/v1/wells/production'.format(self.url), headers=headers, json=production)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Exception('Unable to batch create well production')

  def put_well_production(self, well_id, date, production):
    logger.debug('Creating well production for {} {}: {}'.format(well_id, date, production))
    headers = self._get_headers()
    result = self.session.put('{}/v1/wells/production/{}/{}'.format(self.url, well_id, date), headers=headers, json=production)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Exception('Unable to create well production')

  def list_well_production(self, well_ids = None, facility_ids = None, start_date = None, end_date = None):
    logger.debug('Getting well production')
    headers = self._get_headers()
    params = {}
    if well_ids:
      params['well_ids'] = well_ids
    if facility_ids:
      params['facility_ids'] = facility_ids
    if start_date:
      params['start_date'] = start_date
    if end_date:
      params['end_date'] = end_date
    result = self.session.get('{}/v1/wells/production'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      well_production = result.json()
      logger.debug('Well production: {}'.format(well_production))
      return well_production
    else:
      raise Client_Exception('Unable to retrieve well production')

  def list_well_daily_warehouse(self, well_ids = None, facility_ids = None, start_date = None, end_date = None):
    logger.debug('Getting well warehouse')
    headers = self._get_headers()
    params = {}
    if well_ids:
      params['well_ids'] = well_ids
    if facility_ids:
      params['facility_ids'] = facility_ids
    if start_date:
      params['start_date'] = start_date
    if end_date:
      params['end_date'] = end_date
    result = self.session.get('{}/v1/wells/warehouse'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      well_warehouse = result.json()
      logger.debug('Well warehouse: {}'.format(well_warehouse))
      return well_warehouse
    else:
      raise Client_Exception('Unable to retrieve well warehouse')

  def list_well_status(self, well_ids = None):
    logger.debug('Getting well status')
    headers = self._get_headers()
    params = {}
    if well_ids:
      params['well_ids'] = well_ids
    result = self.session.get('{}/v1/wells/status/latest'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      well_status = result.json()
      logger.debug('Well status: {}'.format(well_status))
      return well_status
    else:
      raise Client_Exception('Unable to retrieve well status')

  def get_well_config(self, well_id):
    logger.debug('Getting config for {}'.format(well_id))
    headers = self._get_headers()
    result = self.session.get('{}/v1/wells/{}/config'.format(self.url, well_id), headers=headers)

    if result.status_code == 200:
      config = result.json()
      logger.debug('config: {}'.format(config))
      return config
    else:
      raise Client_Exception('Unable to retrieve config')

  def get_well_type_curve(self, well_id):
    logger.debug('Getting type curve for {}'.format(well_id))
    headers = self._get_headers()
    result = self.session.get('{}/v1/wells/{}/type-curve'.format(self.url, well_id), headers=headers)

    if result.status_code == 200:
      type_curve = result.json()
      logger.debug('Type curve: {}'.format(type_curve))
      return type_curve
    else:
      raise Client_Exception('Unable to retrieve type curve')

  def get_type_curves(self, well_ids = None, facility_ids = None, lease_ids = None, start_date = None, end_date = None, combine = True):
    logger.debug('Getting type curves')
    headers = self._get_headers()
    params = {}
    if well_ids:
      params['well_ids'] = well_ids
    if facility_ids:
      params['facility_ids'] = facility_ids
    if lease_ids:
      params['lease_ids'] = lease_ids
    if start_date:
      params['start_date'] = start_date
    if end_date:
      params['end_date'] = end_date
    params['combine'] = combine
    result = self.session.get('{}/v1/type-curves'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      curves = result.json()
      logger.debug('Type curves: {}'.format(curves))
      return curves
    else:
      raise Client_Exception('Unable to retrieve type curves')

  def batch_well_type_curve(self, well_id, curves):
    logger.debug('Creating type curve for {}'.format(well_id))
    headers = self._get_headers()
    result = self.session.put('{}/v1/wells/{}/type-curve'.format(self.url, well_id), headers=headers, json=curves)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Client_Exception('Unable to create well type curves')

  def get_well_tpr_ipr_curve(self, well_id, refresh):
    logger.debug('Getting TPR/IPR curve for {}'.format(well_id))
    headers = self._get_headers()
    params = {}
    if refresh:
      params['refresh'] = refresh
    result = self.session.get('{}/v1/wells/{}/tpr-ipr-curve'.format(self.url, well_id), headers=headers, params=params)
    if result.status_code == 200:
      data = result.json()
      logger.debug('TPR/IPR curve data: {}'.format(data))
      return data
    else:
      raise Client_Exception('Unable to retrieve IPR/TPR curve')
      
  def get_res_mgmt_plots(self, well_id, refresh):
    logger.debug('Getting resevior mgmt plot data for {}'.format(well_id))
    headers = self._get_headers()
    params = {}
    if refresh:
      params['refresh'] = refresh
    result = self.session.get('{}/v1/wells/{}/res_mgmt_plots'.format(self.url, well_id), headers=headers, params=params)
    if result.status_code == 200:
      data = result.json()
      logger.debug('resevior mgmt plot data: {}'.format(data))
      return data
    else:
      raise Client_Exception('Unable to retrieve resevior mgmt plot data')
      
  def get_flowing_bottom_hole_pressure(self, well_id, refresh):
    logger.debug('Getting flowing bottom hole pressure history for {}'.format(well_id))
    headers = self._get_headers()
    params = {}
    if refresh:
      params['refresh'] = refresh
    result = self.session.get('{}/v1/wells/{}/flowing-bottom-hole-pressure'.format(self.url, well_id), headers=headers, params=params)
    if result.status_code == 200:
      data = result.json()
      logger.debug('flowing bottom hole pressure history: {}'.format(data))
      return data
    else:
      raise Client_Exception('Unable to retrieve flowing bottom hole pressure history')

  def get_financials_categories(self):
    logger.debug('Getting financials categories')
    headers = self._get_headers()
    result = self.session.get('{}/v1/financials-categories'.format(self.url), headers=headers)

    if result.status_code == 200:
      categories = result.json()
      logger.debug('Financials Categories: {}'.format(categories))
      return categories
    else:
      raise Client_Exception('Unable to retrieve financials categories')

  def post_financials_category(self, category):
    logger.debug('Creating financials category {}'.format(category))
    headers = self._get_headers()
    result = self.session.post('{}/v1/financials-categories'.format(self.url), headers=headers, json=category)
    if result.status_code == 201:
      created = result.json()
      logger.debug('Financials Category: {}'.format(created))
      return created
    else:
      raise Client_Exception('Unable to create financials categories')

  def post_financials_category_price(self, price):
    logger.debug('Creating financials category price {}'.format(price))
    headers = self._get_headers()
    result = self.session.post('{}/v1/financials-categories-price'.format(self.url), headers=headers, json=price)
    if result.status_code == 201:
      created = result.json()
      logger.debug('Financials Category Price: {}'.format(created))
      return created
    else:
      raise Client_Exception('Unable to create financials categories price')

  def get_well_financials_category_prices(self, date, well_ids = None):
    logger.debug('Getting financials categories prices')
    headers = self._get_headers()
    params = {'date': date}
    if well_ids:
      params['well_ids'] = well_ids
    result = self.session.get('{}/v1/financials-categories-well-price'.format(self.url), headers=headers, params=params)

    if result.status_code == 200:
      categories = result.json()
      logger.debug('Financials Categories: {}'.format(categories))
      return categories
    else:
      raise Client_Exception('Unable to retrieve financials categories')

  def put_financials(self, type, type_id, month, financials):
    logger.debug('Putting financials for {} {} {}'.format(type, type_id, month))
    headers = self._get_headers()
    result = self.session.put('{}/v1/financials/{}/{}/{}'.format(self.url, type, type_id, month), headers=headers, json=financials)
    if result.status_code not in [200, 201]:
      logger.exception(result.json())
      raise Client_Exception('Unable to put financials')

  def get_financials(self, asset_type = 'wells', type = 'production', well_ids = None, facility_ids = None, lease_ids = None, start_date = None, end_date = None, start_month = None, end_month = None):
    logger.debug('Getting type financials')
    headers = self._get_headers()
    params = {'asset_type': asset_type}
    params = {'type': type}
    if well_ids:
      params['well_ids'] = well_ids
    if facility_ids:
      params['facility_ids'] = facility_ids
    if lease_ids:
      params['lease_ids'] = lease_ids
    if start_date:
      params['start_date'] = start_date
    if end_date:
      params['end_date'] = end_date
    if start_month:
      params['start_month'] = start_month
    if end_month:
      params['end_month'] = end_month
    result = self.session.get('{}/v1/financials/{}'.format(self.url, asset_type), headers=headers, params=params)
    if result.status_code == 200:
      financials = result.json()
      logger.debug('type financials: {}'.format(financials))
      return financials
    else:
      raise Client_Exception('Unable to retrieve type financials')

  def put_facility_config(self, facility_id, config):
    logger.debug('Putting config for {}'.format(facility_id))
    headers = self._get_headers()
    result = self.session.put('{}/v1/facilities/{}/config'.format(self.url, facility_id), headers=headers, json=config)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Client_Exception('Unable to put facility config')

  def put_facility_sales(self, facility_id, month, sales):
    logger.debug('Putting sales for {} {}'.format(facility_id, month))
    headers = self._get_headers()
    result = self.session.put('{}/v1/facilities/sales/{}/{}'.format(self.url, facility_id, month), headers=headers, json=sales)
    if result.status_code not in [200, 201]:
      logger.exception(result.json())
      raise Client_Exception('Unable to put sales')

  def list_well_sales(self, well_ids=None, start_date=None, end_date=None):
    logger.debug('Getting well sales')
    headers = self._get_headers()
    params = {}
    if well_ids:
      params['well_ids'] = well_ids
    if start_date:
      params['start_date'] = start_date
    if end_date:
      params['end_date'] = end_date
    result = self.session.get('{}/v1/wells/sales/daily'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      well_sales = result.json()
      logger.debug('Well production: {}'.format(well_sales))
      return well_sales
    else:
      raise Client_Exception('Unable to retrieve well sales')

  def put_well_config(self, well_id, config):
    logger.debug('Putting config for {}'.format(well_id))
    headers = self._get_headers()
    result = self.session.put('{}/v1/wells/{}/config'.format(self.url, well_id), headers=headers, json=config)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Client_Exception('Unable to put well config')
    
  def get_strapping_table(self, asset_id, type = 'tanks'):
    logger.debug('Getting strapping table for {} of type: {}'.format(asset_id, type))
    headers = self._get_headers()
    result = self.session.get('{}/v1/{}/{}/strapping'.format(self.url, type, asset_id), headers=headers)
    if result.status_code == 200:
      strapping_table = result.content.decode()
      logger.debug('Strapping Table: {}'.format(strapping_table))
      reader = csv.reader(strapping_table.split('\n'), delimiter=',')
      strapping_table = {float(row[0]):float(row[1]) for row in reader}
      logger.debug('Strapping Table: {}'.format(strapping_table))
      return strapping_table
    else:
      raise Client_Exception('Unable to retrieve strapping table for asset {} of type {}'.format(asset_id, type))

  def batch_put_well_datapoint(self, datapoint):
    logger.debug('Creating well datapoint for {}'.format(datapoint))
    headers = self._get_headers()
    result = self.session.put('{}/v1/wells/datapoint'.format(self.url), headers=headers, json=datapoint)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Exception('Unable to batch create well datapoint')

  def get_well_datapoint(self, well_ids = None, datapoints = None, timestamps = None):
    logger.debug('Getting well datapoint')
    headers = self._get_headers()
    params = {}
    if well_ids:
      params['well_ids'] = well_ids
    if datapoints:
      params['datapoints'] = datapoints
    if timestamps:
      params['timestamps'] = timestamps
    result = self.session.get('{}/v1/wells/datapoint'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      well_datapoint = result.json()
      logger.debug('Well datapoint: {}'.format(well_datapoint))
      return well_datapoint
    else:
      raise Client_Exception('Unable to retrieve well datapoint')
      
  def get_custom_reports(self):
    logger.debug('Getting custom reports list')
    headers = self._get_headers()
    result = self.session.get('{}/v1/custom_reports'.format(self.url), headers=headers)
    if result.status_code == 200:
      reports = result.json()
      logger.debug('custom reports: {}'.format(reports))
      return reports
    else:
      raise Client_Exception('Unable to retrieve custom reports list')
  
  def list_report_tank_gauge(self, well_ids = None, start_date = None, end_date = None):
    logger.debug('Getting tank gauge report list')
    headers = self._get_headers()
    params = {}
    if well_ids:
      params['well_ids'] = well_ids
    if start_date:
      params['start_date'] = start_date
    if end_date:
      params['end_date'] = end_date
    result = self.session.get('{}/v1/wells/report/tank-gauge'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      reports = result.json()
      logger.debug('custom reports: {}'.format(reports))
      return reports
    else:
      raise Client_Exception('Unable to retrieve tank gauge report list')

  def list_monthly_oil_report(self, facility_ids = None, start_month = None, end_month = None):
    logger.debug('Getting oil report list')
    headers = self._get_headers()
    params = {}
    if facility_ids:
      params['facility_ids'] = facility_ids
    if start_month:
      params['start_month'] = start_month
    if end_month:
      params['end_month'] = end_month
    result = self.session.get('{}/v1/facilities/report/oil'.format(self.url), headers=headers, params=params)
    if result.status_code == 200:
      reports = result.json()
      logger.debug('oil reports: {}'.format(reports))
      return reports
    else:
      raise Client_Exception('Unable to retrieve oil report list')

  def send_sms(self, to_numbers, sms_text):
    logger.debug('Sending sms to {}'.format(to_numbers))
    headers = self._get_headers()
    body = { 'to_numbers': to_numbers, 'text': sms_text }
    result = self.session.post('{}/v1/sms'.format(self.url), headers=headers, json=body)
    if result.status_code not in [200, 201]:
      logger.exception(result.json())
      raise Client_Exception('Unable to send sms')