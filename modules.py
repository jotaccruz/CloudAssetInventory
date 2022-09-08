import base64
import json

def get_variables_dynamic(event):

    variables = {}

    if 'data' in event:
        event = base64.b64decode(event['data']).decode('utf-8')
        eventjson = json.loads(event)
        eventdata = eventjson['data']

        if 'SourceProject' in eventdata:
            variables['SourceProject'] = eventdata['SourceProject']
        if 'asset_types' in eventdata:
            variables['asset_types'] = eventdata['asset_types']
        if 'dataset' in eventdata:
            variables['dataset'] = eventdata['dataset']
        if 'table' in eventdata:
            variables['table'] = eventdata['table']
        else:
            variables['table'] = 'assetinventory'
    return variables


#{"SourceProject":["ti-dba-devenv-01","ti-ca-infrastructure"],"asset_types":[".*.googleapis.com.*Instance"]}
