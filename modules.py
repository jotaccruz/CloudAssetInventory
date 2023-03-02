import base64
import json

def get_variables_dynamic(event):

    variables = {}

    if 'data' in event:
        event = base64.b64decode(event['data']).decode('utf-8')
        eventjson = json.loads(event)
        eventdata = eventjson['data']

        if 'Level' in eventdata:
            variables['Level'] = eventdata['Level']
        if 'SourceName' in eventdata:
            variables['SourceName'] = eventdata['SourceName']
        if 'asset_types' in eventdata:
            variables['asset_types'] = eventdata['asset_types']
        if 'dataset' in eventdata:
            variables['dataset'] = eventdata['dataset']
        if 'table' in eventdata:
            variables['table'] = eventdata['table']
        else:
            variables['table'] = 'assetinventory'
    return variables


#{"SourceName":["ti-dba-devenv-01","ti-ca-infrastructure"],"asset_types":[".*.googleapis.com.*Instance"]}
