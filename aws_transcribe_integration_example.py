# Databricks notebook source
%md
## AWS Transcribe Integration Pipeline
This notebook demonstrates how to integrate AWS Transcribe with Databricks for audio transcription processing.

# COMMAND ----------

%md
### Install Required Packages

# COMMAND ----------

%pip install boto3
dbutils.library.restartPython()

# COMMAND ----------

%md
### Import Libraries

# COMMAND ----------

import boto3
import json
import time
import requests
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

%md
### Configuration
Update these values for your environment. Use Databricks Secrets for credentials!

# COMMAND ----------

# AWS Configuration - use secrets for credentials
aws_access_key = dbutils.secrets.get(scope="aws-scope", key="access-key")
aws_secret_key = dbutils.secrets.get(scope="aws-scope", key="secret-key")
aws_region = "us-east-1"

# S3 paths
audio_bucket = "my-audio-files"
audio_prefix = "call-recordings/"
transcribe_output_bucket = "my-transcribe-output"

# Databricks catalog/schema
catalog = "communications"
schema = "transcriptions"
raw_table = f"{catalog}.{schema}.raw_transcriptions"
processed_table = f"{catalog}.{schema}.call_transcripts"

# COMMAND ----------

%md
### Initialize AWS Clients

# COMMAND ----------

# Initialize boto3 clients
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

transcribe_client = boto3.client(
    'transcribe',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

print("✓ AWS clients initialized successfully")

# COMMAND ----------

%md
### Create Database Schema
Create the catalog and schema if they don't exist

# COMMAND ----------

%sql
CREATE CATALOG IF NOT EXISTS communications;
USE CATALOG communications;
CREATE SCHEMA IF NOT EXISTS transcriptions;

# COMMAND ----------

%md
### Discover Audio Files to Process
Find audio files in S3 that haven't been transcribed yet

# COMMAND ----------

def get_unprocessed_audio_files():
    """Get list of audio files from S3 that haven't been processed yet"""
    
    # List objects in S3
    response = s3_client.list_objects_v2(
        Bucket=audio_bucket,
        Prefix=audio_prefix
    )
    
    audio_files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            # Filter for audio files
            if key.endswith(('.mp3', '.mp4', '.wav', '.flac', '.m4a', '.ogg', '.webm')):
                audio_files.append({
                    'file_key': key,
                    'file_name': key.split('/')[-1],
                    'size_bytes': obj['Size'],
                    'last_modified': obj['LastModified']
                })
    
    if not audio_files:
        print("No audio files found in S3")
        return spark.createDataFrame([], schema="file_key STRING, file_name STRING, size_bytes LONG, last_modified TIMESTAMP")
    
    # Create DataFrame
    files_df = spark.createDataFrame(audio_files)
    
    # Check against already processed files (if table exists)
    try:
        processed_df = spark.table(raw_table).select("file_key").distinct()
        unprocessed_df = files_df.join(processed_df, "file_key", "left_anti")
        print(f"Found {unprocessed_df.count()} unprocessed audio files")
    except:
        # Table doesn't exist yet, all files are unprocessed
        unprocessed_df = files_df
        print(f"Found {unprocessed_df.count()} audio files (no existing transcriptions table)")
    
    return unprocessed_df

unprocessed_files = get_unprocessed_audio_files()
display(unprocessed_files)

# COMMAND ----------

%md
### Submit Transcription Jobs to AWS Transcribe
Submit async transcription jobs for each unprocessed audio file

# COMMAND ----------

def start_transcribe_job(file_key, file_name):
    """Start an AWS Transcribe job for a single audio file"""
    
    # Create unique job name
    job_name = f"transcribe_{file_name.replace('.', '_').replace(' ', '_')}_{int(time.time())}"
    media_uri = f"s3://{audio_bucket}/{file_key}"
    
    # Get file format
    file_format = file_name.split('.')[-1].lower()
    # Map file extensions to Transcribe formats
    format_mapping = {
        'mp3': 'mp3',
        'mp4': 'mp4',
        'wav': 'wav',
        'flac': 'flac',
        'm4a': 'mp4',
        'ogg': 'ogg',
        'webm': 'webm'
    }
    media_format = format_mapping.get(file_format, 'mp3')
    
    try:
        response = transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat=media_format,
            LanguageCode='en-US',  # Adjust as needed - supports en-US, es-US, etc.
            OutputBucketName=transcribe_output_bucket,
            Settings={
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': 10,
                'ChannelIdentification': False,
                'ShowAlternatives': False
            }
        )
        
        return {
            'job_name': job_name,
            'status': 'SUBMITTED',
            'file_key': file_key,
            'file_name': file_name,
            'media_uri': media_uri,
            'submit_time': datetime.now()
        }
    except Exception as e:
        return {
            'job_name': job_name,
            'status': 'FAILED',
            'file_key': file_key,
            'file_name': file_name,
            'error': str(e),
            'submit_time': datetime.now()
        }

# Submit jobs for all unprocessed files
submitted_jobs = []
file_count = unprocessed_files.count()

if file_count > 0:
    print(f"Submitting {file_count} transcription jobs...")
    for row in unprocessed_files.collect():
        job_result = start_transcribe_job(row['file_key'], row['file_name'])
        submitted_jobs.append(job_result)
        print(f"  ✓ Submitted: {job_result['job_name']}")
        time.sleep(0.5)  # Small delay for rate limiting
    
    submitted_df = spark.createDataFrame(submitted_jobs)
    display(submitted_df)
else:
    print("No files to process")
    submitted_df = spark.createDataFrame([], schema="job_name STRING, status STRING, file_key STRING, file_name STRING, media_uri STRING, submit_time TIMESTAMP")

# COMMAND ----------

%md
### Monitor Transcription Job Status
Poll AWS Transcribe to check job completion status

# COMMAND ----------

def check_transcription_status(job_name):
    """Check the status of a transcription job"""
    try:
        response = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name
        )
        job = response['TranscriptionJob']
        return {
            'job_name': job_name,
            'status': job['TranscriptionJobStatus'],
            'transcript_uri': job.get('Transcript', {}).get('TranscriptFileUri'),
            'completion_time': job.get('CompletionTime'),
            'failure_reason': job.get('FailureReason')
        }
    except Exception as e:
        return {
            'job_name': job_name,
            'status': 'ERROR',
            'error': str(e)
        }

def poll_for_completion(job_names, max_wait_minutes=30, poll_interval=30):
    """Poll transcription jobs until complete or timeout"""
    
    if not job_names:
        print("No jobs to poll")
        return []
    
    start_time = time.time()
    max_wait_seconds = max_wait_minutes * 60
    completed = []
    
    print(f"Polling {len(job_names)} jobs (max wait: {max_wait_minutes} minutes)...")
    
    while job_names and (time.time() - start_time) < max_wait_seconds:
        remaining = []
        
        for job_name in job_names:
            status = check_transcription_status(job_name)
            
            if status['status'] in ['COMPLETED', 'FAILED']:
                completed.append(status)
                print(f"  {'✓' if status['status'] == 'COMPLETED' else '✗'} {job_name}: {status['status']}")
            else:
                remaining.append(job_name)
        
        job_names = remaining
        
        if job_names:
            elapsed = int(time.time() - start_time)
            print(f"  ⏳ Waiting for {len(job_names)} jobs... ({elapsed}s elapsed, sleeping {poll_interval}s)")
            time.sleep(poll_interval)
    
    if job_names:
        print(f"⚠ Timeout reached. {len(job_names)} jobs still in progress")
    
    return completed

# Get job names from submitted jobs
job_names = [job['job_name'] for job in submitted_jobs if job['status'] == 'SUBMITTED']
completed_jobs = poll_for_completion(job_names, max_wait_minutes=30, poll_interval=30)

if completed_jobs:
    completed_df = spark.createDataFrame(completed_jobs)
    display(completed_df)
else:
    print("No completed jobs to display")

# COMMAND ----------

%md
### Download and Parse Transcription Results
Retrieve the transcription JSON from S3 and extract the text and metadata

# COMMAND ----------

def parse_transcription_result(transcript_uri):
    """Download and parse the transcription JSON from S3"""
    
    # Download the JSON file
    response = requests.get(transcript_uri)
    transcript_data = response.json()
    
    # Extract key information
    results = transcript_data.get('results', {})
    
    return {
        'full_transcript': results.get('transcripts', [{}])[0].get('transcript', ''),
        'items': results.get('items', []),
        'speaker_labels': results.get('speaker_labels', {}).get('segments', []),
        'raw_json': json.dumps(transcript_data)
    }

# Process completed transcriptions
transcription_records = []

print("Processing completed transcriptions...")
for job in completed_jobs:
    if job['status'] == 'COMPLETED' and job.get('transcript_uri'):
        try:
            # Parse the transcription
            parsed = parse_transcription_result(job['transcript_uri'])
            
            # Find original file info
            original_file = next(
                (f for f in submitted_jobs if f['job_name'] == job['job_name']), 
                None
            )
            
            transcription_records.append({
                'job_name': job['job_name'],
                'file_key': original_file['file_key'] if original_file else None,
                'file_name': original_file['file_name'] if original_file else None,
                'transcript_text': parsed['full_transcript'],
                'transcript_json': parsed['raw_json'],
                'transcription_timestamp': job['completion_time'],
                'ingestion_timestamp': datetime.now()
            })
            print(f"  ✓ Parsed: {job['job_name']}")
        except Exception as e:
            print(f"  ✗ Failed to parse {job['job_name']}: {e}")

print(f"\nSuccessfully processed {len(transcription_records)} transcriptions")

# COMMAND ----------

%md
### Write Raw Transcriptions to Delta Table

# COMMAND ----------

if transcription_records:
    # Create DataFrame
    transcripts_df = spark.createDataFrame(transcription_records)
    
    # Write to raw transcriptions table
    transcripts_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(raw_table)
    
    print(f"✓ Wrote {len(transcription_records)} transcriptions to {raw_table}")
    display(transcripts_df.select("job_name", "file_name", "transcript_text", "transcription_timestamp"))
else:
    print("No transcription records to write")

# COMMAND ----------

%md
### Process Transcriptions with Speaker Diarization
Create a structured table with speaker segments extracted from the JSON

# COMMAND ----------

%sql
CREATE OR REPLACE TABLE ${catalog}.${schema}.call_transcripts AS
SELECT 
  job_name,
  file_key,
  file_name,
  transcript_text,
  from_json(
    transcript_json, 
    'results STRUCT<
      speaker_labels: STRUCT<
        segments: ARRAY<STRUCT<
          speaker_label: STRING,
          start_time: STRING,
          end_time: STRING,
          items: ARRAY<STRUCT<
            speaker_label: STRING,
            start_time: STRING,
            end_time: STRING
          >>
        >>
      >,
      items: ARRAY<STRUCT<
        start_time: STRING,
        end_time: STRING,
        alternatives: ARRAY<STRUCT<
          confidence: STRING,
          content: STRING
        >>,
        type: STRING
      >>
    >'
  ).results.speaker_labels.segments as speaker_segments,
  transcription_timestamp,
  ingestion_timestamp,
  current_timestamp() as processed_timestamp
FROM ${catalog}.${schema}.raw_transcriptions
WHERE transcription_timestamp IS NOT NULL

# COMMAND ----------

%md
### View Processed Call Transcripts

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.call_transcripts"))

# COMMAND ----------

%md
### Extract Speaker Utterances (Optional)
Explode speaker segments into individual utterances for analysis

# COMMAND ----------

%sql
CREATE OR REPLACE TABLE ${catalog}.${schema}.speaker_utterances AS
SELECT 
  job_name,
  file_name,
  segment.speaker_label,
  CAST(segment.start_time AS DOUBLE) as start_time_seconds,
  CAST(segment.end_time AS DOUBLE) as end_time_seconds,
  CAST(segment.end_time AS DOUBLE) - CAST(segment.start_time AS DOUBLE) as duration_seconds,
  -- Extract text for this speaker segment (requires joining with items)
  transcript_text,
  transcription_timestamp
FROM ${catalog}.${schema}.call_transcripts
LATERAL VIEW explode(speaker_segments) as segment
ORDER BY job_name, start_time_seconds

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.speaker_utterances"))

# COMMAND ----------

%md
### Analytics: Call Summary Statistics

# COMMAND ----------

%sql
SELECT 
  COUNT(DISTINCT job_name) as total_calls,
  COUNT(DISTINCT speaker_label) as total_speakers,
  ROUND(AVG(duration_seconds), 2) as avg_utterance_duration_sec,
  ROUND(SUM(duration_seconds) / 60, 2) as total_audio_minutes,
  MAX(transcription_timestamp) as last_processed
FROM ${catalog}.${schema}.speaker_utterances

# COMMAND ----------

%md
### Clean Up Transcription Jobs (Optional)
Delete completed jobs from AWS Transcribe to manage quota

# COMMAND ----------

def cleanup_transcribe_jobs(job_names):
    """Delete completed transcription jobs to manage quota"""
    deleted = 0
    failed = 0
    
    for job_name in job_names:
        try:
            transcribe_client.delete_transcription_job(
                TranscriptionJobName=job_name
            )
            deleted += 1
            print(f"  ✓ Deleted: {job_name}")
        except Exception as e:
            failed += 1
            print(f"  ✗ Failed to delete {job_name}: {e}")
    
    print(f"\n✓ Deleted {deleted} jobs, {failed} failures")

# Uncomment the following lines to clean up jobs after successful processing
# completed_job_names = [job['job_name'] for job in completed_jobs if job['status'] == 'COMPLETED']
# if completed_job_names:
#     cleanup_transcribe_jobs(completed_job_names)
# else:
#     print("No completed jobs to clean up")

# COMMAND ----------

%md
## Next Steps

### For Production Use:
1. **Schedule this notebook** as a Databricks Job (hourly/daily)
2. **Set up Databricks Secrets** for AWS credentials
3. **Configure error handling** and retry logic
4. **Add data quality checks** on transcription results
5. **Set up monitoring** and alerting
6. **Consider event-driven architecture** with S3 events → Lambda → Databricks Jobs API

### Advanced Features to Add:
- Custom vocabulary for domain-specific terms
- Real-time streaming with AWS Transcribe Streaming
- Sentiment analysis on transcripts
- PII redaction/masking
- Multi-language support
- Call analytics (talk time, silence detection, etc.)
