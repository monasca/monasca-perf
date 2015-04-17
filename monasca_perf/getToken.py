__author__ = 'ryan'

import sys

from monascaclient import ksclient

keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    'auth_url': 'http://192.168.10.5:35357/v3'
}

if len(sys.argv) >= 2:
    keystone['auth_url'] = int(sys.argv[1])

if len(sys.argv) >= 3:
    keystone['username'] = int(sys.argv[2])

if len(sys.argv) >= 4:
    keystone['password'] = int(sys.argv[3])

if len(sys.argv) >= 5:
    keystone['project'] = int(sys.argv[4])

ks_client = ksclient.KSClient(**keystone)

print("Token: {}".format(ks_client.token))