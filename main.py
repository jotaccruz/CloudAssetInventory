import base64
from google.cloud import asset_v1
import modules
from modules import *


def export_assets(event, context):
    #Pub/Sub Message

    variables = get_variables_dynamic(event)

    #Preparing Source and Target Instance smetadata to be able to run some validations

    # create client
    client = asset_v1.AssetServiceClient()
    print("""This Function was triggered by messageId {} published at {} to {}
    """.format(context.event_id, context.timestamp, context.resource["name"]))

    # bq partition spec
    # PARTITION_KEY_UNSPECIFIED = 0
    # READ_TIME = 1
    # REQUEST_TIME = 2
    partition_spec = asset_v1.PartitionSpec()
    partition_spec.partition_key = 1

    # init request
    output_config = asset_v1.OutputConfig()
    print (variables['dataset'])
    print (variables['table'])
    output_config.bigquery_destination.dataset = variables['dataset']
    #"projects/ti-dba-devenv-01/datasets/AssetInventory"
    output_config.bigquery_destination.table = variables['table']
    #"assetinventory"
    output_config.bigquery_destination.force = False
    output_config.bigquery_destination.partition_spec = partition_spec

    for projects in variables['SourceProject']:
        print (projects)
        #output_config.bigquery_destination.table = projects #"tidbadevenv01"
        request = asset_v1.ExportAssetsRequest(
            parent = "projects/" + projects,
            content_type = "RESOURCE",
            asset_types = variables['asset_types']
            #[
                #".*.googleapis.com.*Cluster",
                #".*.googleapis.com.*Instance",
                #"cloudfunctions.googleapis.com/CloudFunction",
                #"compute.googleapis.com/ForwardingRule",
                #"compute.googleapis.com/VpnTunnel",
                #"container.googleapis.com.*",
                #"iam.googleapis.com.*",
                #"storage.googleapis.com/Bucket",
            #]
            ,output_config = output_config,
        )

        # make request
        operation = client.export_assets(request=request)

        msg_body = base64.b64decode(event['data']).decode('utf-8')
        msg_body = projects
        print('Exporting: {}'.format(msg_body))
        response = operation.result()

        # handle response
        print(response)
