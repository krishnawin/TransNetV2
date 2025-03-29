import os
import subprocess
import ray
import boto3

# Initialize Ray
ray.init(ignore_reinit_error=True)

# AWS S3 configuration
SOURCE_BUCKET_NAME = "yt-vtt"
DESTINATION_FOLDER = "converted_videos"
FPS = 24

access_key = os.getenv('access_key')
secret_key = os.getenv('secret_key')
region = os.getenv('region')
namespace = os.getenv('namespace')
# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
    endpoint_url=f'https://{namespace}.compat.objectstorage.{region}.oraclecloud.com',
)



def download_video(bucket_name, source_key, destination_file_name):
    """Download a video from the S3 bucket."""
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        endpoint_url=f'https://{namespace}.compat.objectstorage.{region}.oraclecloud.com',
    )
    s3_client.download_file(bucket_name, source_key, destination_file_name)
    print(f"Downloaded {source_key} to {destination_file_name}")

def upload_video(bucket_name, source_file_name, destination_key):
    """Upload a video to the S3 bucket."""
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        endpoint_url=f'https://{namespace}.compat.objectstorage.{region}.oraclecloud.com',
    )
    s3_client.upload_file(source_file_name, bucket_name, destination_key)
    print(f"Uploaded {source_file_name} to {destination_key}")

def convert_to_mp4(input_file, output_file, fps):
    """Convert a video to MP4 format with the specified FPS using ffmpeg."""
    command = [
        "ffmpeg",
        "-i", input_file,
        "-r", str(fps),
        "-y",  # Overwrite output file if it exists
        output_file
    ]
    subprocess.run(command, check=True)
    print(f"Converted {input_file} to {output_file} with {fps} FPS")

@ray.remote
def process_video(bucket_name, source_key):
    """Download, convert, and upload a video."""
    try:
        # Temporary file paths
        local_input_file = f"/tmp/{os.path.basename(source_key)}"
        local_output_file = f"/tmp/converted_{os.path.basename(source_key)}"
        # Download video
        download_video(bucket_name, source_key, local_input_file)

        # Convert video to MP4 with 24 FPS
        output_file_mp4 = f'{local_output_file.split(".")[0]}.mp4' # Example for .webm to .mp4
        convert_to_mp4(local_input_file, output_file_mp4, FPS)

        # Upload converted video
        destination_key = f"{DESTINATION_FOLDER}/{os.path.basename(output_file_mp4)}"
        upload_video(bucket_name, output_file_mp4, destination_key)

        # Clean up local files
        os.remove(local_input_file)
        os.remove(output_file_mp4)

        return f"Processed {source_key} successfully."
    except Exception as e:
        return f"Failed to process {source_key}: {e}"

def main():
    # List all videos in the source bucket
    response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET_NAME)
    video_files = [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith((".mp4", ".webm", ".avi",".mkv"))
    ]
    # Process videos in parallel using Ray
    futures = [process_video.remote(SOURCE_BUCKET_NAME, video) for video in video_files]
    results = ray.get(futures)

    # Print results
    for result in results:
        print(result)

if __name__ == "__main__":
    main()