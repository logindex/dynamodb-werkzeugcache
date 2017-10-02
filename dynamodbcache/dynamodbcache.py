# -*- coding: utf-8 -*-
import logging
from collections import namedtuple
from time import time, sleep

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    boto3 = ClientError = None  # noqa

from werkzeug.contrib.cache import BaseCache

DynamoDBAttribute = namedtuple('DynamoDBAttribute', ('name', 'data_type'))

def dynamodbcache(app, config, args, kwargs):
    """Auxiliary function to be used in results backend in Celery"""
    return DynamodDbCache(*args, **kwargs)

class DynamodDbCache(BaseCache):

    """Uses AWS Dynamo Db table as a cache backend.

    The constructor can receive all the boto3 client parameters, of if none passed relies an all usual
    resolution of boto3.

    Table will have the following columns:
    * id (sting) key of the cache
    * result (any) Any pickled object to be cached
    * timestamp (numeric) Epoch datetime when object was cached
    * expiry (numeric) Epoch date time by which object expires

    Note that objects do not expire automatically at that time. DynamoDb can take up to 48 hours to expire it, so
    while expiry is good to guarantee that objects are autodeleted, the expiry field should be checked upon
    retrieval.

    :param table_name: Name of the dynamodb table to create
    :param all remaining: Parameters to boto3 client
    """

    _key_field = DynamoDBAttribute(name='id', data_type='S')
    _value_field = DynamoDBAttribute(name='result', data_type='B')
    _timestamp_field = DynamoDBAttribute(name='timestamp', data_type='N')
    _expiration_field = DynamoDBAttribute(name='expiry', data_type='N')
    _available_fields = None

    def __init__(self, table_name, access_key_id=None, secret_access_key=None, aws_region=None,
                 endpoint_url=None, default_timeout=300, read_capacity_units=1, write_capacity_units=1, **kwargs):

        BaseCache.__init__(self, default_timeout)
        try:
            import boto3
        except ImportError:
            raise RuntimeError('no boto3 module found')
        if kwargs.get('decode_responses', None):
            raise ValueError('decode_responses is not supported by RedisCache.')

        self.table_name=table_name
        self.access_key_id= access_key_id
        self.secret_access_key = secret_access_key
        self.aws_region = aws_region
        self.endpoint_url = endpoint_url
        self.default_timeout = default_timeout
        self.read_capacity_units = read_capacity_units
        self.write_capacity_units = write_capacity_units
        self._client = None

    @property
    def client(self):
        return self._get_client(self.access_key_id,self.secret_access_key)

    def _get_client(self, access_key_id=None, secret_access_key=None):
        """Laty getter of client"""

        if self._client is None:

            client_parameters = { 'region_name': self.aws_region }
            if access_key_id is not None:
                client_parameters.update({
                    'aws_access_key_id': access_key_id,
                    'aws_secret_access_key': secret_access_key
                })

            if self.endpoint_url is not None:
                client_parameters['endpoint_url'] = self.endpoint_url

            self._client = boto3.client('dynamodb', **client_parameters)
            self._get_or_create_table()

        return self._client

    def _get_or_create_table(self):
        """Create table if not exists, otherwise return the description."""

        table_schema = self._get_table_schema()
        try:
            table_description = self.client.create_table(**table_schema)
            logging.info('DynamoDB Table {} did not exist, creating.'.format(self.table_name))

            # In case we created the table, wait until it becomes available.
            self._wait_for_table_status('ACTIVE')
            logging.info('DynamoDB Table {} is now available.'.format(self.table_name))

            self.client.update_time_to_live(
                TableName=self.table_name,
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': self._expiration_field.name
                }
            )
            logging.info('DynamoDB Table {} now expires items'.format(self.table_name))

            return table_description

        except ClientError as e:
            error_code = e.response['Error'].get('Code', 'Unknown')

            # If table exists, do not fail, just return the description.
            if error_code == 'ResourceInUseException':
                return self.client.describe_table(TableName=self.table_name)
            else:
                raise e

    def _get_table_schema(self):
        """Get the boto3 structure describing the DynamoDB table schema."""

        return {
            'AttributeDefinitions': [
                {
                    'AttributeName': self._key_field.name,
                    'AttributeType': self._key_field.data_type
                }
            ],
            'TableName': self.table_name,
            'KeySchema': [
                {
                    'AttributeName': self._key_field.name,
                    'KeyType': 'HASH'
                }
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': self.read_capacity_units,
                'WriteCapacityUnits': self.write_capacity_units
            }
        }


    def _wait_for_table_status(self, expected='ACTIVE'):
        """Poll for the expected table status."""

        achieved_state = False
        while not achieved_state:
            table_description = self.client.describe_table(TableName=self.table_name)
            logging.debug('Waiting for DynamoDB table {} to become {}.'.format(self.table_name,expected))
            current_status = table_description['Table']['TableStatus']
            achieved_state = current_status == expected
            sleep(1)

    def _item_to_dict(self, raw_response):
        """Convert get_item() response to field-value pairs."""

        if 'Item' not in raw_response:
            return {}
        return {
            field.name: raw_response['Item'][field.name][field.data_type]
            for field in self._available_fields
        }

    def dump_object(self, value):
        """
        Dumps an object into a string for redis.  By default it serializes integers as regular string and
        pickle dumps everything else.
        """
        return b'!' + pickle.dumps(value)

    def load_object(self, value):
        """The reversal of :meth:`dump_object`.  This might be called with None."""

        if value is None:
            return None
        try:
            return pickle.loads(value[1:])
        except pickle.PickleError:
            return None

    def get(self, key):
        """Returns the DynamoDB item for key"""
        try:
            return self.load_object(
                self._item_to_dict(
                    self.client.get_item(
                        **self._prepare_get_request(str(key))
                    )
                ).get(
                    self._value_field.name
                )
            )
        except Exception as e:
            logging.info('Error getting object to DynamoDB table %s : %s'.format(self.table_name,e))
            return None

    def set(self, key, value, timeout=None):
        try:
            self.client.put_item(**self._prepare_put_request(str(key), value, time(), timeout, True))
        except Exception as e:
            logging.info('Error adding object to DynamoDB table %s : %s'.format(self.table_name,e))

    def add(self, key, value, timeout=None):
        try:
            self.client.put_item(**self._prepare_put_request(str(key), value, time(), timeout, False))
        except Exception as e:
            logging.info('Error adding object to DynamoDB table %s : %s'.format(self.table_name, e))

    def delete(self, key):
        try:
            self.client.delete_item(**self._prepare_get_request(str(key)))
        except Exception as e:
            logging.info('Error deleting object to DynamoDB table %s : %s'.format(self.table_name, e))

    def clear(self):
        raise NotImplementedError('clear() is not implemented in DynamoDbCache')

    def _prepare_put_request(self, key, value, timestamp, timeout, put_if_exists):
        """Construct the item creation request parameters."""
        req = {
            'TableName': self.table_name,
            'Item': {
                self._key_field.name: {
                    self._key_field.data_type: key
                },
                self._value_field.name: {
                    self._value_field.data_type: value
                },
                self._timestamp_field.name: {
                    self._timestamp_field.data_type: str(int(timestamp))
                },
                self._expiration_field.name: {
                    self._expiration_field.data_type: str(int(timestamp+timeout))
                }
            }
        }

        if not put_if_exists:
            req.update({
                'ConditionExpression' : 'attribute_not_exists(%s)' % self._key_field.name
            })

        return req

    def _prepare_get_request(self, key):
        """Construct the item retrieval request parameters."""

        return {
            'TableName': self.table_name,
            'Key': {
                self._key_field.name: {
                    self._key_field.data_type: key
                }
            }
        }
