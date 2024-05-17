from google.cloud import storage
import pandas as pd
import io
import pickle

def process_pickle(blob):
    try:
        # Read the pickle content as bytes
        blob_content = blob.download_as_string()

        # Create a file-like object from the bytes content
        file_obj = io.BytesIO(blob_content)

        # Unpickle the file content to get a dictionary
        d = pickle.load(file_obj)

        # Initialize a list to store extracted data
        data_list = []

        # Iterate over each key in the dictionary
        for key, data in d.items():
            # Check if 'track' information is present
            if 'track' in data:
                track_info = data['track']
                # Extract desired attributes
                row_data = {
                    'key': key,
                    'track_uri': data.get('track_uri', ''),
                    'num_samples': track_info.get('num_samples', ''),
                    'duration': track_info.get('duration', ''),
                    'loudness': track_info.get('loudness', ''),
                    'tempo': track_info.get('tempo', ''),
                    'time_signature': track_info.get('time_signature', ''),
                }
                # Append extracted data to the list
                data_list.append(row_data)

        return data_list

    except EOFError:
        print(f"Error: EOFError encountered while processing {blob.name}. Skipping this file.")
        return []

if __name__ == '__main__':
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Define your GCS bucket name and folder path containing the pickle files
    bucket_name = 'my-bucket-mpat'
    folder_path = 'landing/'

    # List all files in the GCS bucket with prefix 'spotify_merged_'
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path + 'spotify_merged_')

    # Convert blobs to a list to process in parallel
    blob_list = [blob for blob in blobs if blob.name.endswith('.pkl')]

    # Process each pickle file in parallel and collect the extracted data
    extracted_data_list = []
    for blob in blob_list:
        extracted_data_list.extend(process_pickle(blob))

    # Create a DataFrame from the list of extracted dictionaries
    df = pd.DataFrame(extracted_data_list)

    # Save the DataFrame to a CSV file in Google Cloud Storage
    output_filename = 'outputSPM2.csv'
    csv_output_path = 'gs://{}/landing/{}'.format(bucket_name, output_filename)
    df.to_csv(csv_output_path, index=False)

    print('CSV file saved to:', csv_output_path)
