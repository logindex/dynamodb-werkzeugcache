#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Logindex AG ("COMPANY") CONFIDENTIAL
Unpublished Copyright (c) 2009-2017 Logindex, All Rights Reserved.

NOTICE:  All information contained herein is, and remains the property of COMPANY. The intellectual and technical
concepts contained herein are proprietary to COMPANY and may be covered by U.S. and Foreign Patents, patents in process,
and are protected by trade secret or copyright law.
Dissemination of this information or reproduction of this material is strictly forbidden unless prior written
permission is obtained from COMPANY.  Access to the source code contained herein is hereby forbidden to anyone except
current COMPANY employees, managers or contractors who have executed Confidentiality and Non-disclosure agreements
explicitly covering such access.

The copyright notice above does not evidence any actual or intended publication or disclosure  of  this source code,
which includes information that is confidential and/or proprietary, and is a trade secret, of  COMPANY.
ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE, OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE
CODE  WITHOUT  THE EXPRESS WRITTEN CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE LAWS AND
INTERNATIONAL TREATIES.  THE RECEIPT OR POSSESSION OF  THIS SOURCE CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR
IMPLY ANY RIGHTS TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT
MAY DESCRIBE, IN WHOLE OR IN PART.
"""
import logging
import pickle
from base64 import b64encode, b64decode
from collections import namedtuple
from time import time, sleep
try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    boto3 = ClientError = None  # noqa

from werkzeug._compat import iteritems
from werkzeug.contrib.cache import BaseCache

def _items(mappingorseq):
    """Wrapper for efficient iteration over mappings represented by dicts
    or sequences::

        >>> for k, v in _items((i, i*i) for i in xrange(5)):
        ...    assert k*k == v

        >>> for k, v in _items(dict((i, i*i) for i in xrange(5))):
        ...    assert k*k == v

    """
    if hasattr(mappingorseq, 'items'):
        return iteritems(mappingorseq)
    return mappingorseq

DynamoDBAttribute = namedtuple('DynamoDBAttribute', ('name', 'data_type'))

class DynamodDbCache(BaseCache):

    """Uses the Redis key-value store as a cache backend.

    The first argument can be either a string denoting address of the Redis
    server or an object resembling an instance of a redis.Redis class.

    Note: Python Redis API already takes care of encoding unicode strings on
    the fly.

    .. versionadded:: 0.7

    .. versionadded:: 0.8
       `key_prefix` was added.

    .. versionchanged:: 0.8
       This cache backend now properly serializes objects.

    .. versionchanged:: 0.8.3
       This cache backend now supports password authentication.

    .. versionchanged:: 0.10
        ``**kwargs`` is now passed to the redis object.

    :param host: address of the Redis server or an object which API is
                 compatible with the official Python Redis client (redis-py).
    :param port: port number on which Redis server listens for connections.
    :param password: password authentication for the Redis server.
    :param db: db (zero-based numeric index) on Redis Server to connect.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param key_prefix: A prefix that should be added to all keys.

    Any additional keyword arguments will be passed to ``redis.Redis``.
    """

    _key_field = DynamoDBAttribute(name='id', data_type='S')
    _value_field = DynamoDBAttribute(name='result', data_type='B')
    _timestamp_field = DynamoDBAttribute(name='timestamp', data_type='N')
    _expiry_field = DynamoDBAttribute(name='expiry', data_type='N')
    _available_fields = [_value_field, _timestamp_field, _expiry_field]

    def __init__(self, table_name, access_key_id=None, secret_access_key=None, aws_region=None,
                 endpoint_url=None, default_timeout=300, read_capacity_units=1, write_capacity_units=1, **kwargs):

        BaseCache.__init__(self, default_timeout)

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
            logging.info('DynamoDB Table %s did not exist, creating.',self.table_name)

            # In case we created the table, wait until it becomes available.
            self._wait_for_table_status('ACTIVE')
            logging.info('DynamoDB Table %s is now available.',self.table_name)

            self.client.update_time_to_live(
                TableName=self.table_name,
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': self._expiry_field.name
                }
            )
            logging.info('DynamoDB Table %s now expires items',self.table_name)

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
            logging.debug('Waiting for DynamoDB table %s to become %s.',self.table_name,expected)
            current_status = table_description['Table']['TableStatus']
            achieved_state = current_status == expected
            sleep(1)

    def _item_to_dict(self, raw_response):
        """Convert get_item() response to field-value pairs."""

        if 'Item' not in raw_response:
            return {}

        return {
            field.name: raw_response['Item'][field.name][field.data_type] for field in self._available_fields
        }

    def dump_object(self, value):
        """
        Dumps an object into a string for redis.  By default it serializes integers as regular string and
        pickle dumps everything else.
        """
        return pickle.dumps(value)

    def load_object(self, value):
        """The reversal of :meth:`dump_object`.  This might be called with None."""

        if value is None:
            return None

        try:
            return pickle.loads(value)
        except pickle.PickleError as e:
            logging.error('Pickle error: %s',e)

    def get(self, key):
        """Returns the DynamoDB item for key"""
        try:

            item = self._item_to_dict(self.client.get_item(**self._prepare_get_request(str(key))))

            # If item is empty, nothing in cache
            if not item:
                return None

            # If current time beyond expiry, nothing to return
            if time()>float(item[self._expiry_field.name]):
                return None

            return self.load_object(b64decode(item.get(self._value_field.name)))

        except Exception as e:
            logging.info('Error getting object from DynamoDB table %s (%s): %s',self.table_name,e.__class__.__name__,e)
            return None

    def set(self, key, value, timeout=None):
        try:
            self.client.put_item(**self._prepare_put_request(str(key), self.dump_object(value), time(), timeout, True))
        except Exception as e:
            logging.info('Error adding object to DynamoDB table %s : %s',self.table_name,e)

    def add(self, key, value, timeout=None):
        try:
            self.client.put_item(**self._prepare_put_request(str(key), self.dump_object(value), time(), timeout, False))
        except Exception as e:
            logging.info('Error adding object to DynamoDB table %s : %s',self.table_name, e)

    def delete(self, key):
        try:
            self.client.delete_item(**self._prepare_get_request(str(key)))
        except Exception as e:
            logging.info('Error deleting object to DynamoDB table %s : %s',self.table_name, e)

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
                    self._value_field.data_type: b64encode(value).decode('ascii')
                },
                self._timestamp_field.name: {
                    self._timestamp_field.data_type: str(int(timestamp))
                },
                self._expiry_field.name: {
                    self._expiry_field.data_type: str(int(timestamp + timeout))
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