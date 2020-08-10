import os
import logging
import requests

logger = logging.getLogger('sotaog_public_api_client')
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))


class Client_Exception(Exception):
  pass


class Client():
  def __init__(self, url: str, client_id: str, client_secret: str, customer_id: str = None):
    self.session = requests.Session()
    self.url = url.rstrip('/')
    self.customer_id = customer_id
    logger.info(f'Initializing Sotaog API client for {url}')
    logger.debug(f'Authenticating to API: {url}')
    data = {
        'grant_type': 'client_credentials'
    }
    result = self.session.post(f'{self.url}/v1/authenticate', data=data, auth=(client_id, client_secret))
    if result.status_code == 200:
      self.token = result.json()['access_token']
      logger.debug(f'Token: {self.token}')
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
    logger.debug(f'Getting alarm services')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/alarm-services', headers=headers)
    if result.status_code == 200:
      alarm_services = result.json()
      logger.debug(f'Alarm Services: {alarm_services}')
      return alarm_services
    else:
      raise Client_Exception(f'Unable to retrieve alarm services')

  def get_alarm_service(self, alarm_service_id: str):
    logger.debug(f'Getting alarm service {alarm_service_id}')
    headers = self._get_headers()
    url = f'{self.url}/v1/alarm-services/{alarm_service_id}'
    result = self.session.get(url, headers=headers)
    if result.status_code == 200:
      alarm_service = result.json()
      logger.debug(f'Alarm Service: {alarm_service}')
      return alarm_service
    else:
      raise Client_Exception(f'Unable to retrieve alarm service {alarm_service_id}')

  def get_alarms(self):
    logger.debug(f'Getting alarms')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/alarms', headers=headers)
    if result.status_code == 200:
      alarms = result.json()
      logger.debug(f'Alarms: {alarms}')
      return alarms
    else:
      raise Client_Exception(f'Unable to retrieve alarms')

  def get_alarm(self, asset_id: str, datatype: str = None):
    logger.debug(f'Getting alarms for {asset_id}')
    headers = self._get_headers()
    url = f'{self.url}/v1/alarms/{asset_id}'
    if datatype:
      url += f'/{datatype}'
    result = self.session.get(url, headers=headers)
    if result.status_code == 200:
      alarm = result.json()
      logger.debug(f'Alarm: {alarm}')
      return alarm
    else:
      raise Client_Exception(f'Unable to retrieve alarms for {asset_id}')

  def get_facilities(self):
    logger.debug(f'Getting facilities')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/facilities', headers=headers)
    if result.status_code == 200:
      facilities = result.json()
      logger.debug(f'Facilities: {facilities}')
      return facilities
    else:
      raise Client_Exception(f'Unable to retrieve facilities')

  def get_facility(self, facility_id: str):
    logger.debug(f'Getting facility: {facility_id}')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/facilities/{facility_id}', headers=headers)
    if result.status_code == 200:
      facility = result.json()
      logger.debug(f'Facility: {facility}')
      return facility
    else:
      raise Client_Exception(f'Unable to retrieve facility {facility_id}')

  def get_asset(self, asset_id, type: str = 'assets'):
    logger.debug(f'Getting asset {asset_id} of type: {type}')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/{type}/{asset_id}', headers=headers)
    if result.status_code == 200:
      asset = result.json()
      logger.debug(f'Asset: {asset}')
      return asset
    else:
      raise Client_Exception(f'Unable to retrieve asset {asset_id} of type {type}')

  def get_assets(self, type: str = 'assets', facility: str = None, asset_type: str = None):
    logger.debug(f'Getting assets of type: {type}')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/{type}', headers=headers)
    if result.status_code == 200:
      assets = result.json()
      if facility:
        assets = [asset for asset in assets if 'facility' in asset and asset['facility'] == facility]
      if asset_type:
        assets = [asset for asset in assets if 'asset_type' in asset and asset['asset_type'] == asset_type]
      logger.debug(f'Assets: {assets}')
      return assets
    else:
      raise Client_Exception(f'Unable to retrieve assets of type {asset_type}')

  def get_asset_type(self, asset_type_id):
    logger.debug(f'Getting asset type {asset_type_id}')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/asset-types/{asset_type_id}', headers=headers)
    if result.status_code == 200:
      asset_type = result.json()
      logger.debug(f'Asset Type: {asset_type}')
      return asset_type
    else:
      raise Client_Exception(f'Unable to retrieve asset {asset_type_id}')

  def get_asset_types(self):
    logger.debug('Getting asset types')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/asset-types', headers=headers)
    if result.status_code == 200:
      asset_types = result.json()
      logger.debug(f'Asset types: {asset_types}')
      return asset_types
    else:
      raise Client_Exception('Unable to get asset types')

  def get_customers(self):
    logger.debug('Getting customers')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/customers', headers=headers)
    if result.status_code == 200:
      customers = result.json()
      logger.debug(f'Customers: {customers}')
      return customers
    else:
      raise Client_Exception('Unable to get customers')

  def get_customer(self, customer_id: str):
    logger.debug(f'Getting customer {customer_id}')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/customers/{customer_id}', headers=headers)
    if result.status_code == 200:
      customer = result.json()
      logger.debug(f'Customer: {customer}')
      return customer
    else:
      raise Client_Exception(f'Unable to get customer {customer_id}')

  def get_datatypes(self, group_by='asset'):
    logger.debug('Getting datatypes')
    headers = self._get_headers()
    params = {}
    if group_by:
      params['group_by'] = group_by
    result = self.session.get(f'{self.url}/v1/datatypes', headers=headers, params=params)
    if result.status_code == 200:
      datatypes = result.json()
      logger.debug(f'Datatypes: {datatypes}')
      return datatypes
    else:
      raise Client_Exception('Unable to get datatypes')

  def get_datatype(self, datatype_id):
    logger.debug(f'Getting datatype {datatype_id}')
    headers = self._get_headers()
    params = {'group_by': 'asset'}
    result = self.session.get(f'{self.url}/v1/datatypes/{datatype_id}', headers=headers, params=params)
    if result.status_code == 200:
        datatype = result.json()
        logger.debug(f'Datatype: {datatype}')
        return datatype
    else:
        raise Client_Exception(f'Unable to get datatype {datatype_id}')

  def get_datapoints(self, asset_datatypes: dict, start_ts: int = None, end_ts: int = None, sort: str = 'desc', limit: int = 100):
    logger.debug(f'Getting datapoints for asset_datatypes: {asset_datatypes}')
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
    result = self.session.post(f'{self.url}/v1/datapoints', headers=headers, json=body)
    if result.status_code == 200:
      datapoints = result.json()
      logger.debug(f'Datapoints: {datapoints}')
      return datapoints
    else:
      logger.debug(result.json())
      raise Client_Exception('Unable to get datapoints')

  def get_asset_datapoints(self, asset_id: str, datatypes: [str] = [], start_ts: int = None, end_ts: int = None, sort: str = 'desc', limit: int = 100):
    logger.debug(f'Getting datapoints for asset: {asset_id}')
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
    result = self.session.get(f'{self.url}/v1/datapoints/{asset_id}', headers=headers, params=params)
    if result.status_code == 200:
      datapoints = result.json()
      logger.debug(f'Datapoints: {datapoints}')
      return datapoints
    else:
      raise Client_Exception('Unable to get datapoints')

  def get_swd_networks(self, facility: str = None):
    logger.debug(f'Getting SWD networks')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/swd-networks', headers=headers)
    if result.status_code == 200:
      swd_networks = result.json()
      logger.debug(f'SWD Networks: {swd_networks}')
      if facility:
        swd_networks = [swd_network for swd_network in swd_networks if facility in swd_network['facilities']]
      logger.debug(f'SWD Networks: {swd_networks}')
      return swd_networks
    else:
      raise Client_Exception(f'Unable to retrieve SWD networks')

  def get_truck_tickets(self, facility: str = None, type: str = None, start_ts: int = None, end_ts: int = None):
    logger.debug(f'Getting truck tickets')
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
    result = self.session.get(f'{self.url}/v1/truck-tickets', headers=headers, params=params)
    if result.status_code == 200:
      truck_tickets = result.json()
      logger.debug(f'Truck tickets: {truck_tickets}')
      return truck_tickets
    else:
      raise Client_Exception(f'Unable to retrieve truck tickets')

  def post_truck_ticket(self, truck_ticket: object):
    logger.debug(f'Creating truck ticket {truck_ticket}')
    headers = self._get_headers()
    result = self.session.post(f'{self.url}/v1/truck-tickets', headers=headers, json=truck_ticket)
    if result.status_code == 201:
      created_ticket = result.json()
      logger.debug(f'Truck ticket: {created_ticket}')
      return created_ticket
    else:
      raise Client_Exception(f'Unable to create truck ticket')

  def put_truck_ticket_image(self, truck_ticket_id: str, image: bytes, content_type: str):
    logger.debug(f'Creating truck ticket image size {len(image)}, content_type {content_type}')
    headers = self._get_headers()
    headers['content-type'] = content_type
    result = self.session.put(f'{self.url}/v1/truck-tickets/{truck_ticket_id}/image', headers=headers, data=image)
    if result.status_code != 204:
      logger.exception(result.json())
      raise Client_Exception(f'Unable to create truck ticket image')

  def put_alarm(self, asset_id: str, datatype: str, alarm: object):
    logger.debug(f'Creating alarm for {asset_id} {datatype}')
    headers = self._get_headers()
    result = self.session.put(f'{self.url}/v1/alarms/{asset_id}/{datatype}', headers=headers, json=alarm)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Exception('Unable to create alarm')

  def post_datapoints(self, asset_id, datapoints):
    logger.debug(f'Posting datapoints')
    headers = self._get_headers()
    result = self.session.post(f'{self.url}/v1/datapoints/{asset_id}', headers=headers, json=datapoints)
    if result.status_code != 202:
      logger.exception(result.json())
      raise Exception('Unable to post datapoints')

  def batch_put_well_production(self, production: list):
    logger.debug(f'Creating well production for {production}')
    headers = self._get_headers()
    result = self.session.put(f'{self.url}/v1/wells/production', headers=headers, json=production)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Exception('Unable to batch create well production')

  def put_well_production(self, well_id: str, date: str, production: object):
    logger.debug(f'Creating well production for {well_id} {date}: {production}')
    headers = self._get_headers()
    result = self.session.put(f'{self.url}/v1/wells/production/{well_id}/{date}', headers=headers, json=production)
    if result.status_code != 201:
      print(result.text)
      logger.exception(result.json())
      raise Exception('Unable to create well production')

  def list_well_production(self, well_ids: list = None, facility_ids: list = None, start_date: str = None, end_date: str = None):
    logger.debug(f'Getting well production')
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
    result = self.session.get(f'{self.url}/v1/wells/production', headers=headers, params=params)
    if result.status_code == 200:
      well_production = result.json()
      logger.debug(f'Well production: {well_production}')
      return well_production
    else:
      raise Client_Exception(f'Unable to retrieve well production')

  def get_well_config(self, well_id: str):
    logger.debug(f'Getting config for {well_id}')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/wells/{well_id}/config', headers=headers)

    if result.status_code == 200:
      config = result.json()
      logger.debug(f'config: {config}')
      return config
    else:
      raise Client_Exception(f'Unable to retrieve config')

  def get_well_type_curve(self, well_id: str):
    logger.debug(f'Getting type curve for {well_id}')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/wells/{well_id}/type-curve', headers=headers)

    if result.status_code == 200:
      type_curve = result.json()
      logger.debug(f'Type curve: {type_curve}')
      return type_curve
    else:
      raise Client_Exception(f'Unable to retrieve type curve')

  def put_well_type_curve(self, well_id: str, curve: object):
    logger.debug(f'Creating type curve for {well_id}')
    headers = self._get_headers()
    result = self.session.put(f'{self.url}/v1/wells/{well_id}/type-curve', headers=headers, json=curve)
    if result.status_code != 201:
      logger.exception(result.json())
      raise Client_Exception('Unable to create well type curve')

  def get_financials_categories(self):
    logger.debug('Getting financials categories')
    headers = self._get_headers()
    result = self.session.get(f'{self.url}/v1/financials-categories', headers=headers)

    if result.status_code == 200:
      categories = result.json()
      logger.debug(f'Financials Categories: {categories}')
      return categories
    else:
      raise Client_Exception(f'Unable to retrieve financials categories')

  def post_financials_category(self, category: object):
    logger.debug(f'Creating financials category {category}')
    headers = self._get_headers()
    result = self.session.post(f'{self.url}/v1/financials-categories', headers=headers, json=category)
    if result.status_code == 201:
      created = result.json()
      logger.debug(f'Financials Category: {created}')
      return created
    else:
      raise Client_Exception(f'Unable to create financials categories')

  def put_financials(self, type: str, type_id: str, month: str, financials):
    logger.debug(f'Putting financials for {type} {type_id} {month}')
    headers = self._get_headers()
    result = self.session.put(f'{self.url}/v1/financials/{type}/{type_id}/{month}', headers=headers, json=financials)
    if result.status_code not in [200, 201]:
      logger.exception(result.json())
      raise Client_Exception('Unable to put financials')

  def put_facility_sales(self, facility_id: str, month: str, sales):
    logger.debug(f'Putting sales for {facility_id} {month}')
    headers = self._get_headers()
    result = self.session.put(f'{self.url}/v1/facilities/sales/{facility_id}/{month}', headers=headers, json=sales)
    if result.status_code not in [200, 201]:
      logger.exception(result.json())
      raise Client_Exception('Unable to put sales')
