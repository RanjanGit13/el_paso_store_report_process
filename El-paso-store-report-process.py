# Imports
import boto3
import json
from botocore.exceptions import ClientError
import pandas as pd 
import numpy
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from datetime import datetime, timedelta, time
import os
import tempfile
from hashlib import sha256
from io import StringIO, BytesIO
from dateutil.relativedelta import relativedelta
import pytz

# testing
print("**********HELLO WORLD***********")

def delete_old_objects_storedreport_config(bucket_name='el-paso-storedreport', folder_path='storedreport-config', months=6):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Calculate the date 6 months ago
    six_months_ago = datetime.now(pytz.utc) - relativedelta(months=months)

    # List objects in the specified folder path
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

    # Delete objects older than 6 months
    for obj in response.get('Contents', []):
        # Get the object key and last modified date
        object_key = obj['Key']
        last_modified = obj['LastModified'].replace(tzinfo=pytz.utc)  # Convert to UTC timezone

        # Check if the object is older than 6 months
        if last_modified < six_months_ago:
            # Delete the object
            s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"Deleted object: {object_key}")
        else:
            print(f"Skipping object: {object_key}")

    print("Old objects deletion completed.")
    
def delete_old_objects_storedreports(bucket_name='el-paso-storedreport', folder_path='storedreports', months=6):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Calculate the date 6 months ago
    six_months_ago = datetime.now(pytz.utc) - relativedelta(months=months)

    # List objects in the specified folder path
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

    # Delete objects older than 6 months
    for obj in response.get('Contents', []):
        # Get the object key and last modified date
        object_key = obj['Key']
        last_modified = obj['LastModified'].replace(tzinfo=pytz.utc)  # Convert to UTC timezone

        # Check if the object is older than 6 months
        if last_modified < six_months_ago:
            # Delete the object
            s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"Deleted object: {object_key}")
        else:
            print(f"Skipping object: {object_key}")

    print("Old objects deletion completed.")

# encoding and saving params
def save_updated_params(combined_params, username, bucket_name='el-paso-storedreport', folder_path='storedreport-config'):

    # Calculate the SHA256 hash of the JSON data
    params_hash = sha256(json.dumps(combined_params, sort_keys=True).encode()).hexdigest()

    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Specify the file key using the hashed params
    file_key = f'{folder_path}/{username}/{params_hash}.json'

    # Check if the JSON file already exists in S3
    try:
        s3.head_object(Bucket=bucket_name, Key=file_key)
        print(f"JSON file for '{params_hash}' with same configuration already exists in S3.")
    except Exception as e:
        # Save the updated JSON data as a JSON file in S3
        try:
            s3.put_object(Body=json.dumps(combined_params), Bucket=bucket_name, Key=file_key)
            print(f"JSON file for '{params_hash}' saved successfully in S3 under '{folder_path}'.")
        except Exception as e:
            print(f"Error saving JSON file in S3: {e}")

# saving to s3
def save_df_to_s3(df, json_dict, hash_key, bucket_name='el-paso-storedreport', folder_path='storedreports'):

    username = json_dict.get('username', 'default_username')
    date_type = json_dict.get('date_type', 'default_date_type')
    start_date = json_dict.get('start_date', 'default_start_date')
    start_date = json_dict.get('end_date', 'default_start_date')

    if date_type=="Departure Date":
        tag_name = "Revenue"
    else:
        tag_name = "Sales"

    file_name = f"{username}_{tag_name}_{datetime.now().strftime('%Y%m%d')}_{hash_key}"

    if file_name is None:
        raise ValueError("File name cannot be None")

    # Convert Snowpark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Create a buffer to store the CSV data
    csv_buffer = StringIO()

    # Write the DataFrame to the buffer as CSV
    pandas_df.to_csv(csv_buffer, index=False)

    # Convert the buffer content to bytes
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Upload the CSV data to S3
    file_key = f'{folder_path}/{username}/{file_name}.csv'
    s3.put_object(Body=csv_bytes, Bucket=bucket_name, Key=file_key)

    print(f"CSV file '{file_name}.csv' saved successfully in S3.")

# fetch json from s3
def retrieve_json_files_from_s3(bucket_name='el-paso-storedreport', folder_path='storedreport-config'):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Get a list of objects in the specified folder path
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

    # Loop through each object in the response
    for obj in response.get('Contents', []):
        # Get the object key
        object_key = obj['Key']

        # Download the JSON file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)

        # Read the JSON data from the response
        json_data = response['Body'].read().decode('utf-8')
        
        # Convert JSON string to dictionary
        try:
            json_dict = json.loads(json_data)
            print(type(json_dict))
            
            # status check
            if not json_dict['complete_status']:
                # Create Query and Dataframe
                query = generate_query_filter(json_dict)  # Use json_dict instead of params
                final_df = data_aggregator(
                    get_data_from_snowflake(query),
                    json_dict
                )
                print(final_df.show())
                print(final_df.count())

                # save the csv
                hash_key = object_key.split("/")[-1][:-5]
                save_df_to_s3(final_df, json_dict, hash_key, bucket_name='el-paso-storedreport', folder_path='storedreports')

                # status change
                json_dict['complete_status'] = True
                s3.put_object(Body=json.dumps(json_dict), Bucket=bucket_name, Key=object_key)

                # Update and save the JSON data
                if "start_date" in json_dict:
                    # Convert the "start_date" value to datetime object
                    json_dict["start_date"] = datetime.strptime(json_dict["start_date"], '%Y-%m-%d')
                    # Add one day to the "start_date"
                    json_dict["start_date"] += timedelta(days=1)
                    # Convert the updated "start_date" back to string format
                    json_dict["start_date"] = json_dict["start_date"].strftime('%Y-%m-%d')

                if "end_date" in json_dict:
                    # Convert the "end_date" value to datetime object
                    json_dict["end_date"] = datetime.strptime(json_dict["end_date"], '%Y-%m-%d')
                    # Add one day to the "end_date"
                    json_dict["end_date"] += timedelta(days=1)
                    # Convert the updated "end_date" back to string format
                    json_dict["end_date"] = json_dict["end_date"].strftime('%Y-%m-%d')

                if "complete_status" in json_dict:
                    json_dict['complete_status'] = False

                if "username" in json_dict:
                    username = json_dict["username"]
                else:
                    username = "unknown"

                # Save the updated JSON data to S3
                updated_json_data = json_dict
                save_updated_params(updated_json_data, username, bucket_name='el-paso-storedreport', folder_path='storedreport-config')

                print("Status: Successfull")
            else:
                pass
        except json.JSONDecodeError as e:
            print("Error decoding JSON data:", e)

# Fetch Snowflake credentials
def get_secret():
    secret_name = "snowflake/flairmaster"
    region_name = "ca-central-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']

        # Your code goes here.
        return text_secret_data

# ---------------------------data process----------------------------------------------#
def generate_query_count_rev_pax(condition):
    if condition != "'No Show'":
        condition_query = f"""
        CASE WHEN departure_date > CURRENT_DATE() - INTERVAL '1 DAY' 
                                AND booking_status = 'Confirmed' 
                                AND REVENUE_CLASS = 'Revenue' 
                                AND INFANT_FLAG != 1 
                            THEN lng_Res_Legs_Id_Nmbr 

                            WHEN departure_date <= CURRENT_DATE() - INTERVAL '1 DAY' 
                                AND ( (REVENUE_FLAG = 1 AND PAX_STATUS IN ('Boarded', 'No Show')) 
                                    OR (booking_status = 'Confirmed' AND PAX_STATUS = 'Not Checked In') ) 
                                AND REVENUE_CLASS = 'Revenue' 
                                AND INFANT_FLAG != 1
                            THEN lng_Res_Legs_Id_Nmbr ELSE NULL 
                            END AS lng_Res_Legs_Id_Nmbr_add
                        """
    else:
        condition_query = f"""
         CASE WHEN departure_date > CURRENT_DATE() - INTERVAL '1 DAY' 
                                AND booking_status = 'Confirmed' 
                                AND REVENUE_CLASS = 'Revenue' 
                                AND INFANT_FLAG != 1 
                            THEN lng_Res_Legs_Id_Nmbr 

                            WHEN departure_date <= CURRENT_DATE() - INTERVAL '1 DAY' 
                                AND ( (REVENUE_FLAG = 1 AND PAX_STATUS IN ('Boarded')) 
                                    OR (booking_status = 'Confirmed' AND PAX_STATUS = 'Not Checked In') ) 
                                AND REVENUE_CLASS = 'Revenue' 
                                AND INFANT_FLAG != 1
                            THEN lng_Res_Legs_Id_Nmbr ELSE NULL 
                            END AS lng_Res_Legs_Id_Nmbr_add
                        """
    return condition_query

def generate_query_count_departures():
    condition_query = f""" CASE 
            WHEN FLIGHT_STATUS IN ('X', 'NA', NULL, '') THEN 0 
            ELSE 1 
        END AS DEPARTURES
        """
    return condition_query


def generate_query_filter(params_dataframe):
    combined_params = params_dataframe
    date_type = combined_params["date_type"]
    start_date = combined_params["start_date"]
    end_date = combined_params["end_date"]
    sales_from = combined_params["sales_from"]
    sales_to = combined_params["sales_to"]

    start_time_str = datetime.strptime(combined_params["departure_time"][0], '%H:%M:%S')
    end_time_str = datetime.strptime(combined_params["departure_time"][1], '%H:%M:%S')

    secondary = combined_params["Secondary_date"]
    #snapshot_date = combined_params["snapshot_date"]
    snapshot_date = datetime.now().strftime('%Y-%m-%d')
    #---------------------------------------advance params----------------------------------------
    no_show = combined_params["exclude_no_show"]
    #add rev_pax to table
    rev_pax_query = generate_query_count_rev_pax(no_show)
    #add departure to table
    depature_query = generate_query_count_departures()

    #check if want to add time range filter later
    base_revenue_query = f"""SELECT *,
        {depature_query},
        {rev_pax_query},
        FROM FCT_RELIA
        WHERE CAST(departure_date AS timestamp) >= timestamp '{start_date}'
        AND CAST(departure_date AS timestamp) <= timestamp '{end_date}'
        AND ((departure_time BETWEEN '{start_time_str}' AND '{end_time_str}') OR departure_time is null) """

    base_sales_query = f"""SELECT *,
        {depature_query},
        {rev_pax_query},
        FROM FCT_RELIA
        WHERE CAST(sales_date AS timestamp) >= timestamp '{sales_from}'
        AND CAST(sales_date AS timestamp) <= timestamp '{sales_to}'
        AND CAST(departure_date AS timestamp) >= timestamp '{sales_from}';"""

    if combined_params["date_type"] == "Departure Date":
        query = base_revenue_query
        if secondary:
            query += f""" AND CAST("sales_date" AS timestamp) >= timestamp '{sales_from}' 
            AND CAST("sales_date" AS timestamp) <= timestamp '{sales_to}'"""
        if snapshot_date:
            query = f"""
                SELECT CAST('{snapshot_date}' AS DATE) as snapshot_date, *,
                {depature_query},
                {rev_pax_query},
                FROM FCT_RELIA
                WHERE departure_date >= TIMESTAMP '{start_date}'
                AND departure_date <= TIMESTAMP '{end_date}'
                AND  CAST(sales_date AS TIMESTAMP) <= TIMESTAMP '{snapshot_date}'
                AND ((departure_time BETWEEN '{start_time_str}' AND '{end_time_str}') OR departure_time IS NULL)

                UNION ALL

                SELECT current_date as snapshot_date, *,
                {depature_query},
                {rev_pax_query},
                FROM FCT_RELIA
                WHERE departure_date >= TIMESTAMP '{start_date}'
                AND departure_date <= TIMESTAMP '{end_date}'
                AND ((departure_time BETWEEN '{start_time_str}' AND '{end_time_str}') OR departure_time IS NULL)

                """
    elif combined_params["date_type"] == "Sales Date":
        query = base_sales_query

    #filter region,subregion,ndod
    region_list = combined_params["region"]
    sub_region_list = combined_params["sub_region"]
    ndod_list = combined_params["ndod"]

    regions_str = ', '.join([f"'{region}'" if region is not None else 'NULL' for region in region_list])
    sub_regions_str = ', '.join([f"'{sub_region}'" if sub_region is not None else 'NULL' for sub_region in sub_region_list])
    ndod_str = ', '.join([f"'{ndod}'" if ndod is not None else 'NULL' for ndod in ndod_list])

    if not ndod_str:
        ndod_condition = ""  # Set an empty string if ndod_str is null or empty
    else:
        ndod_condition = f"AND NDOD in ({ndod_str})"  # Include the NDOD condition otherwise

    if not regions_str:
        regions_condition = ""  # Set an empty string if regions_str is null or empty
    else:
        regions_condition = f"AND REGION in ({regions_str})"  # Include the REGION condition otherwise

    if not sub_regions_str:
        sub_regions_condition = ""  # Set an empty string if sub_regions_str is null or empty
    else:
        sub_regions_condition = f"AND SUB_REGION in ({sub_regions_str})"  # Include the SUB_REGION condition otherwise

    # Construct additional_conditions based on the conditions for each parameter
    additional_conditions = f"""{regions_condition} {sub_regions_condition} {ndod_condition}"""
    query = query.replace(f"""((departure_time BETWEEN '{start_time_str}' AND '{end_time_str}') OR departure_time IS NULL)""", 
                            f"((departure_time BETWEEN '{start_time_str}' AND '{end_time_str}') OR departure_time IS NULL){additional_conditions}")


    #filter 0 depatures
    if combined_params["exclude_zero_departures"]:
        try:
            query = query.replace("UNION ALL","AND DEPARTURES = 1 UNION ALL")
            query += "AND DEPARTURES = 1"
        except:
            query += "AND DEPARTURES = 1"


    if combined_params["macro2"] == "PNR":
        return query
    return query

def excute_query(query):
    config_json = json.loads(get_secret())
    config_json.update({'warehouse':'ELPASO_WH', 'database':'SHARED_ELPASO','schema': 'REVENUE_SALES',"loglevel":'DEBUG',"role":"ACCOUNTADMIN"})
    session = Session.builder.configs(config_json).create()
    df = session.sql(query)

    return df

def data_clean(df):
    df = df.na.fill({"MILES": 13})
    #df = df.withColumn("MILES", F.coalesce(df["MILES"], 13))
    if "lng_Res_Legs_Id_Nmbr_add" in df.columns:
        df.drop("lng_Res_Legs_Id_Nmbr_add", axis=1, inplace=True)
    return df

def get_data_from_snowflake(query):
    df = excute_query(query)
    df = data_clean(df)
    return df

def data_aggregator(data, params_dataframe):
    if params_dataframe["date_type"] == "Departure Date":
        if "SNAPSHOT_DATE" in data.columns:
            flight_lv_data = data.groupBy(
                "REGION", "SUB_REGION", "NDOD", "DOD", "DEPARTURE_DATE", "FLIGHT_NO","SNAPSHOT_DATE"
            ).agg(F.max('DEPARTURE_AIRPORT').alias('DEPARTURE_AIRPORT'),
                F.max('ARRIVAL_AIRPORT').alias('ARRIVAL_AIRPORT'),
                F.max('DEPARTURE_TIME').alias('DEPARTURE_TIME'),
                F.max('ARRIVAL_TIME').alias('ARRIVAL_TIME'),
                F.max('BLOCK_HOURS').alias('BLOCK_HOURS'),
                F.countDistinct('lng_Res_Legs_Id_Nmbr_add').alias('REVENUE_PAX_SEG'),
                F.max('DEPARTURES').alias('DEPARTURES'),
                F.max('MILES').alias('MILES'),
                F.sum('PASSENGER_REV').alias('TOTAL_PASSENGER_REVENUE'),
                F.max("FLIGHT_SEATS").alias('FLIGHT_SEATS'),
                F.max("SELLING_CAPACITY").alias('SELLING_CAPACITY'),
                #make the max change here
                F.max("ANCILLARY_REVENUE_SOLD_TILL_DATE").alias('TOTAL_ANCILLARY_REVENUE'))

        else:
            # Initial flight level aggregation
            # Perform groupBy and aggregation

            flight_lv_data = data.groupBy(
                "REGION", "SUB_REGION", "NDOD", "DOD", "DEPARTURE_DATE", "FLIGHT_NO"
            ).agg(F.max('DEPARTURE_AIRPORT').alias('DEPARTURE_AIRPORT'),
                F.max('SALES_DATE').alias('SALES_DATE'),
                F.max('ARRIVAL_AIRPORT').alias('ARRIVAL_AIRPORT'),
                F.max('DEPARTURE_TIME').alias('DEPARTURE_TIME'),
                F.max('ARRIVAL_TIME').alias('ARRIVAL_TIME'),
                F.max('BLOCK_HOURS').alias('BLOCK_HOURS'),
                F.countDistinct('lng_Res_Legs_Id_Nmbr_add').alias('REVENUE_PAX_SEG'),
                F.max('DEPARTURES').alias('DEPARTURES'),
                F.max('MILES').alias('MILES'),
                F.sum('PASSENGER_REV').alias('TOTAL_PASSENGER_REVENUE'),
                F.max("FLIGHT_SEATS").alias('FLIGHT_SEATS'),
                F.max("SELLING_CAPACITY").alias('SELLING_CAPACITY'),
                F.max("ANCILLARY_REV_FLIGHT").alias('TOTAL_ANCILLARY_REVENUE'))

    else:
        # Initial flight level aggregation

        # Perform groupBy and aggregation
        flight_lv_data = data.groupBy(
            "REGION", "SUB_REGION", "NDOD", "DOD", "DEPARTURE_DATE","SALES_DATE", "FLIGHT_NO"
        ).agg(F.max('DEPARTURE_AIRPORT').alias('DEPARTURE_AIRPORT'),
            F.max('ARRIVAL_AIRPORT').alias('ARRIVAL_AIRPORT'),
            #comment for now since departure filter not workign
            F.max('DEPARTURE_TIME').alias('DEPARTURE_TIME'),
            F.max('ARRIVAL_TIME').alias('ARRIVAL_TIME'),
            F.max('BLOCK_HOURS').alias('BLOCK_HOURS'),
            F.countDistinct('lng_Res_Legs_Id_Nmbr_add').alias('REVENUE_PAX_SEG'),
            F.max('DEPARTURES').alias('DEPARTURES'),
            F.max('MILES').alias('MILES'),
            F.sum('PASSENGER_REV').alias('TOTAL_PASSENGER_REVENUE'),
            F.max("FLIGHT_SEATS").alias('FLIGHT_SEATS'),
            F.max("SELLING_CAPACITY").alias('SELLING_CAPACITY'),
            F.max("ANCILLARY_REV_FLIGHT").alias('TOTAL_ANCILLARY_REVENUE'))

        # Convert DEPARTURE_DATE to datetime and extract day, day of week, month, and year
    flight_lv_data = flight_lv_data.withColumn("DEPARTURE_DATE_DAY", F.dayofmonth("DEPARTURE_DATE").cast("string"))
    #changed to same as previous DOW
    #flight_lv_data = flight_lv_data.withColumn("DEPARTURE_DATE_DOW", F.expr("(dayofweek(DEPARTURE_DATE) + 1) % 7").cast("string"))
    flight_lv_data = flight_lv_data.withColumn("DEPARTURE_DATE_DOW", F.dayofweek("DEPARTURE_DATE").cast("string"))
    flight_lv_data = flight_lv_data.withColumn("DEPARTURE_DATE_MONTH", F.month("DEPARTURE_DATE").cast("string"))
    flight_lv_data = flight_lv_data.withColumn("DEPARTURE_DATE_YEAR", F.year("DEPARTURE_DATE").cast("string"))

    # Calculate ASMS and RPMS
    flight_lv_data = flight_lv_data.withColumn("ASMS", flight_lv_data.MILES * flight_lv_data.FLIGHT_SEATS)
    flight_lv_data = flight_lv_data.withColumn("RPMS", flight_lv_data.MILES * flight_lv_data.REVENUE_PAX_SEG)

    # handling macros
    agg_col = []
    if "System" in params_dataframe["macro1"]:
        pass
    if "Region" in params_dataframe["macro1"]:
        agg_col.extend(["REGION"])
    if "Sub Region" in params_dataframe["macro1"]:
        agg_col.extend(["SUB_REGION"])
    # if "Distance Category" in params_dataframe["macro1"]:
    #     agg_col.extend(["DISTANCE_CATEGORY"])
    if "NDOD" in params_dataframe["macro1"]:
        agg_col.extend(["NDOD"])

    agg_dict = {
    "REVENUE_PAX_SEG": F.sum,
    "DEPARTURES": F.sum,
    "MILES": F.sum,
    "TOTAL_PASSENGER_REVENUE": F.sum,
    "FLIGHT_SEATS": F.sum,
    "SELLING_CAPACITY": F.sum,
    "ASMS": F.sum,
    "RPMS": F.sum,
    "BLOCK_HOURS": F.sum,
    "TOTAL_ANCILLARY_REVENUE": F.sum
}

    agg_expr = [F.sum(col_name).alias(col_name) for col_name, func in agg_dict.items()]

    if params_dataframe["date_aggregate"] == "Day of Week - Departure":
        if "SNAPSHOT_DATE" in flight_lv_data.columns:
            group_data = (flight_lv_data.groupBy(
                ["DEPARTURE_DATE_YEAR",
                        "DEPARTURE_DATE_MONTH",
                        "DEPARTURE_DATE_DOW","SNAPSHOT_DATE"] + agg_col
            )
            .agg(*agg_expr))
        else:
            group_data = (
                flight_lv_data.groupBy(
                    [
                        "DEPARTURE_DATE_YEAR",
                        "DEPARTURE_DATE_MONTH",
                        "DEPARTURE_DATE_DOW",
                    ]
                    + agg_col
                )
                .agg(*agg_expr)
            )

        group_data = group_data.orderBy(["DEPARTURE_DATE_YEAR", "DEPARTURE_DATE_MONTH","DEPARTURE_DATE_DOW"], ascending=[True,True,True])

    elif params_dataframe["date_aggregate"] == "Month - Departure":
        if "SNAPSHOT_DATE" in flight_lv_data.columns:
            group_data = (flight_lv_data.groupBy(
                ["DEPARTURE_DATE_YEAR", "DEPARTURE_DATE_MONTH","SNAPSHOT_DATE"] + agg_col
            )
            .agg(*agg_expr))
        else:
            group_data = (flight_lv_data.groupBy(
                ["DEPARTURE_DATE_YEAR", "DEPARTURE_DATE_MONTH"] + agg_col
            )
            .agg(*agg_expr))
        group_data = group_data.orderBy(["DEPARTURE_DATE_YEAR", "DEPARTURE_DATE_MONTH"], ascending=[True,True])

    elif params_dataframe["date_aggregate"] == "Sales Date":
        group_data = (
            flight_lv_data.groupBy(
                [ "SALES_DATE"]
            )
            .agg(*agg_expr)
        )
    #only when date_aggregate based depature date will use snapshot date, since snapshot date is specifically a date
    elif params_dataframe["date_aggregate"] == "Departure Date":
        if "SNAPSHOT_DATE" in flight_lv_data.columns:
            group_data = (flight_lv_data.groupBy(["DEPARTURE_DATE","SNAPSHOT_DATE"])
            .agg(*agg_expr)
        )
        else:
            group_data = (flight_lv_data.groupBy(["DEPARTURE_DATE"])
                .agg(*agg_expr)
            )
    elif params_dataframe["macro2"] == "Flight Number":
         group_data = flight_lv_data
    elif params_dataframe["macro2"] == "PNR":
        group_data = data


    # Additional features
    if params_dataframe["macro2"] != "PNR":
        group_data = group_data.withColumn("DISTANCE_IN_MILE", group_data.MILES)
        # group_data["RPMS"] = (group_data["MILES"]) * group_data["REVENUE_PAX_SEG"]

        group_data = group_data.drop("MILES")
        group_data = group_data.withColumn("TOTAL_REVENUE",
                                       group_data.TOTAL_PASSENGER_REVENUE + group_data.TOTAL_ANCILLARY_REVENUE)

        group_data = group_data.withColumn("TRASM",
                                        F.when(group_data.ASMS == 0, 0)
                                        .otherwise(group_data.TOTAL_REVENUE / group_data.ASMS).cast("float"))

        group_data = group_data.withColumn("PRASM",
                                        F.when(group_data.ASMS == 0, 0)
                                        .otherwise(group_data.TOTAL_PASSENGER_REVENUE / group_data.ASMS).cast("float"))

        group_data = group_data.withColumn("REVENUE_LOAD_FACTOR",
                                        F.when(group_data.ASMS == 0, 0)
                                        .otherwise(group_data.RPMS / group_data.ASMS).cast("float"))

        group_data = group_data.withColumn("AVERAGE_FARE",
                                        F.when(group_data.REVENUE_PAX_SEG == 0, 0)
                                        .otherwise(group_data.TOTAL_PASSENGER_REVENUE / group_data.REVENUE_PAX_SEG).cast("float"))

        group_data = group_data.withColumn("AVERAGE_ANCILLARY",
                                        F.when(group_data.REVENUE_PAX_SEG == 0, 0)
                                        .otherwise(group_data.TOTAL_ANCILLARY_REVENUE / group_data.REVENUE_PAX_SEG).cast("float"))

        group_data = group_data.withColumn("TOTAL_AVERAGE_FARES",
                                        group_data.AVERAGE_FARE + group_data.AVERAGE_ANCILLARY)

        # Convert to float32
        numeric_columns = ["REVENUE_PAX_SEG", "TOTAL_PASSENGER_REVENUE", "FLIGHT_SEATS",
                        "SELLING_CAPACITY", "ASMS", "TOTAL_ANCILLARY_REVENUE", "DISTANCE_IN_MILE",
                        "RPMS", "TOTAL_REVENUE", "TRASM", "PRASM", "REVENUE_LOAD_FACTOR", "AVERAGE_FARE",
                        "AVERAGE_ANCILLARY", "TOTAL_AVERAGE_FARES", "DEPARTURES"]
        for col_name in numeric_columns:
            try:
                group_data = group_data.withColumn(col_name, group_data[col_name].cast("float"))
            except ValueError as e:
                    print(f"Error converting column '{col_name}' to float32: {e}")

        if "BLOCK_HOURS" in group_data.columns:
            group_data = group_data.withColumn("BLOCK_HOURS", group_data.BLOCK_HOURS.cast("float"))

        # Convert to string
        #string_columns = ["DEPARTURE_DATE","FLIGHT_NO" "ARRIVAL_TIME"]
        string_columns = [ "ARRIVAL_TIME"]
        for col_name in string_columns:
            if col_name in group_data.columns:
                group_data = group_data.withColumn(col_name, group_data[col_name].cast("string"))

        # Round columns
        rounding_columns = ["TOTAL_PASSENGER_REVENUE", "TOTAL_ANCILLARY_REVENUE",
                            "TOTAL_REVENUE", "AVERAGE_FARE", "AVERAGE_ANCILLARY", "TOTAL_AVERAGE_FARES"]
        for col_name in rounding_columns:
            if col_name in group_data.columns:
                group_data = group_data.withColumn(col_name, F.round(group_data[col_name], 2))

        columns_to_round = ["AVERAGE_FARE", "AVERAGE_ANCILLARY", "BLOCK_HOURS", "TOTAL_AVERAGE_FARES","TRASM","PRASM"]
        for col_name in columns_to_round:
            group_data = group_data.withColumn(col_name, F.round (group_data[col_name], 1))

        if params_dataframe["subtotals"]:
            numeric_columns = [col_name for col_name, data_type in group_data.dtypes if data_type.startswith("double") or data_type.startswith("float")]

            # Calculate sum or average
            sum_avg_expr = {col_name: "sum" if col_name not in ["REVENUE_LOAD_FACTOR", "AVERAGE_FARE"] else "avg" for col_name in numeric_columns}

            # Create an empty row with NaN values for categorical columns
            last_row = group_data.select(numeric_columns).agg(sum_avg_expr).withColumn("SUBTOTAL", lit("SUM/AVERAGE")).withColumnRenamed("avg", "SUM/AVERAGE")

            # Append the last row to the DataFrame
            group_data = group_data.union(last_row)
        if "SNAPSHOT_DATE" in group_data.columns:
            group_data = group_data.orderBy(["SNAPSHOT_DATE"], ascending=[True])

        return group_data
    print(group_data.columns)
    if "SNAPSHOT_DATE" in group_data.columns:
            group_data = group_data.orderBy(["DEPARTURE_DATE","FLIGHT_NO", "SNAPSHOT_DATE"], ascending=[True,True,True])
    return group_data

#--------------------Main Run--------------------------------------------#
if __name__ == "__main__":
    retrieve_json_files_from_s3()
    delete_old_objects_storedreport_config()
    delete_old_objects_storedreports()