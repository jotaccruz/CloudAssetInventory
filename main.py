import base64
from google.cloud import asset_v1

def CloudAssetInventory(event, context):
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
    output_config.bigquery_destination.dataset = "projects/ti-dba-devenv-01/AssetInventory/tidbadevenv01"
    output_config.bigquery_destination.table = "tidbadevenv01"
    output_config.bigquery_destination.force = True
    output_config.bigquery_destination.partition_spec = partition_spec

    request = asset_v1.ExportAssetsRequest(
        parent = "projects/ti-dba-devenv-01",
        content_type = "RESOURCE",
        asset_types = [
            #".*.googleapis.com.*Cluster",
            ".*.googleapis.com.*Instance",
            #"cloudfunctions.googleapis.com/CloudFunction",
            #"compute.googleapis.com/ForwardingRule",
            #"compute.googleapis.com/VpnTunnel",
            #"container.googleapis.com.*",
            #"iam.googleapis.com.*",
            #"storage.googleapis.com/Bucket",
        ],
        output_config = output_config,
    )

    # make request
    operation = client.CloudAssetInventory(request=request)

    msg_body = base64.b64decode(event['data']).decode('utf-8')
    print('Exporting: {}'.format(msg_body))
    response = operation.result()

    # handle response
    print(response)
