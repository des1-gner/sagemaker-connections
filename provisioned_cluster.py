import boto3
import pandas as pd
import json
import time

def connect_provisioned_cluster():
    """Connect to provisioned Redshift cluster"""
    
    # Get credentials from secret
    secrets_client = boto3.client('secretsmanager', region_name='<aws-region>')
    secret_response = secrets_client.get_secret_value(
        SecretId='arn:aws:secretsmanager:<aws-region>:<aws-account-id>:secret:redshift!<workgroup>-<user>-<identifier>'
    )
    secret = json.loads(secret_response['SecretString'])
    
    # Execute query
    redshift_data = boto3.client('redshift-data', region_name='<aws-region>')
    response = redshift_data.execute_statement(
        ClusterIdentifier='<cluster-identifier>',
        Database='<your-database-name>',
        DbUser=secret['username'],
        Sql='SELECT current_database() as database, current_user as user;'
    )
    
    # Wait for results
    query_id = response['Id']
    while True:
        status = redshift_data.describe_statement(Id=query_id)
        if status['Status'] == 'FINISHED':
            result = redshift_data.get_statement_result(Id=query_id)
            break
        elif status['Status'] == 'FAILED':
            print(f"Query failed: {status.get('Error')}")
            return None
        time.sleep(1)
    
    # Convert to DataFrame
    if result.get('Records'):
        columns = [col['name'] for col in result['ColumnMetadata']]
        data = []
        for record in result['Records']:
            row = []
            for field in record:
                if 'stringValue' in field:
                    row.append(field['stringValue'])
                elif 'longValue' in field:
                    row.append(field['longValue'])
                else:
                    row.append(None)
            data.append(row)
        
        df = pd.DataFrame(data, columns=columns)
        print("Provisioned cluster connection successful")
        print(df)
        return df
    
    return None

# Test provisioned connection
connect_provisioned_cluster()
