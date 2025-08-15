import boto3
import pandas as pd
import json
import time

def connect_serverless():
    """Connect to Redshift Serverless"""
    
    redshift_data = boto3.client('redshift-data', region_name='<aws-region>')
    
    # Execute query using admin secret
    response = redshift_data.execute_statement(
        WorkgroupName='<your-work-group-name>',
        Database='<your-database-name>',
        SecretArn='arn:aws:secretsmanager:<aws-region>:<aws-account-id>:secret:redshift!<cluster-identifer>-<user>-<identifier>',
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
        print("Serverless connection successful")
        print(df)
        return df
    
    return None

# Test serverless connection
connect_serverless()
