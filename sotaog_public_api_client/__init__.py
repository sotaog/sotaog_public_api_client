import os
import logging
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
      logger.debug('Facility: {}', facility)
      return facility
    else:
      raise Client_Exception('Unable to retrieve facility {}'.format(facility_id))

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

  def put_truck_ticket_image(self, truck_ticket_id, image, content_type):
    logger.debug('Creating truck ticket image size {}, content_type {}'.format(len(image), content_type))
    headers = self._get_headers()
    headers['content-type'] = content_type
    result = self.session.put('{}/v1/truck-tickets/{}/image'.format(self.url, truck_ticket_id), headers=headers, data=image)
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
      print(result.text)
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

  def put_well_type_curve(self, well_id, curve):
    logger.debug('Creating type curve for {}'.format(well_id))
    headers = self._get_headers()
    result = self.session.put('{}/v1/wells/{}/type-curve'.format(self.url, well_id), headers=headers, json=curve)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Client_Exception('Unable to create well type curve')

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

  def put_financials(self, type, type_id, month, financials):
    logger.debug('Putting financials for {} {} {}'.format(type, type_id, month))
    headers = self._get_headers()
    result = self.session.put('{}/v1/financials/{}/{}/{}'.format(self.url, type, type_id, month), headers=headers, json=financials)
    if result.status_code not in [200, 201]:
      logger.exception(result.json())
      raise Client_Exception('Unable to put financials')

  def put_facility_sales(self, facility_id, month, sales):
    logger.debug('Putting sales for {} {}'.format(facility_id, month))
    headers = self._get_headers()
    result = self.session.put('{}/v1/facilities/sales/{}/{}'.format(self.url, facility_id, month), headers=headers, json=sales)
    if result.status_code not in [200, 201]:
      logger.exception(result.json())
      raise Client_Exception('Unable to put sales')

  def put_well_config(self, well_id, config):
    logger.debug('Putting config for {}'.format(well_id))
    headers = self._get_headers()
    result = self.session.put('{}/v1/wells/{}/config'.format(self.url, well_id), headers=headers, json=config)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Client_Exception('Unable to put well config')
