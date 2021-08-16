import requests
from tetpyclient import MultiPartOption, RestClient
import os
from tempfile import NamedTemporaryFile
import pandas as pd
from time import sleep

url = os.getenv('TET_URL')
api_key = os.getenv('TET_KEY')
api_secret = os.getenv('TET_SECRET')
tenant_name = os.getenv('TET_TENANT')
update_interval = 60

# Initialization
rc = RestClient(url,api_key=api_key,api_secret=api_secret)

# If script is being run without saved state, download current list of labels from the target cluster, and save to variable
with NamedTemporaryFile(mode="w") as tf:
    resp = rc.download(tf.name, '/openapi/v1/assets/cmdb/download/{}'.format(tenant_name))
    df = pd.read_csv(tf.name)

# Check for the presence of a BlockList column.  If it doesn't exist, assume current list is empty.
if 'BlockList' not in df.columns:
    current_labeled_exit_nodes = {}
else:
    current_labeled_exit_nodes = set(list(df[df['BlockList']=='Tor']['IP']))

# Start loop
while True:
    # Get current exit node list
    print('Getting latest Tor Exit Node IP list from torproject.org...')
    resp = requests.get('https://check.torproject.org/torbulkexitlist')
    current_tor_exit_nodes = set(resp.text.split('\n'))
    current_tor_exit_nodes.remove('')
    print('Downloaded {} currently active exit node IPs'.format(len(current_tor_exit_nodes)))

    # Calculate delta and add new labeled IPs
    nodes_to_add = current_tor_exit_nodes - current_labeled_exit_nodes
    if len(nodes_to_add)>0:
        df = pd.DataFrame()
        df['IP']=list(nodes_to_add)
        df['BlockList']='Tor'

        with NamedTemporaryFile(mode="w") as tf:
            df.to_csv(tf.name, index=False)
            req_payload = [
                                MultiPartOption(
                                    key='X-Tetration-Oper', val='add')
                            ]
            resp = rc.upload(
                tf.name, '/openapi/v1/assets/cmdb/upload/{}'.format(
                    tenant_name), req_payload)
            if resp.ok:
                print("Uploaded {} Tor Exit Node Labels".format(len(nodes_to_add)))
            else:
                print("Failed to Upload Annotations")
                print(resp.text)
    else:
        print('No exit nodes need to be added.  The list is current.')

    # Calculate delta and delete unlabeled IPs
    nodes_to_delete = current_labeled_exit_nodes - current_tor_exit_nodes
    if len(nodes_to_delete)>0:
        df = pd.DataFrame()
        df['IP']=list(nodes_to_delete)
        df['BlockList']='Tor'
        with NamedTemporaryFile(mode="w") as tf:
            df.to_csv(tf.name, index=False)
            req_payload = [
                                MultiPartOption(
                                    key='X-Tetration-Oper', val='delete')
                            ]
            resp = rc.upload(
                tf.name, '/openapi/v1/assets/cmdb/upload/{}'.format(
                    tenant_name), req_payload)
            if resp.ok:
                print("Deleted {} Tor Exit Node Labels".format(len(nodes_to_delete)))
            else:
                print("Failed to Upload Annotations")
                print(resp.text)
    else:
        print('No exit nodes need to be removed.  The list is current.')
    
    # Update to current list
    current_labeled_exit_nodes = current_tor_exit_nodes
    print('Waiting {} seconds to check for exit node changes'.format(update_interval))
    sleep(update_interval)