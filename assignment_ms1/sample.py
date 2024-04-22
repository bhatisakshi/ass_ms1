import paramiko
import os
import shutil
import sqlite3
from tabulate import tabulate
from pydub import AudioSegment
from pydub.exceptions import CouldntDecodeError
from datetime import datetime
import time
import logging
import stat
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Function to create the database and tables if they don't exist
def create_database(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Create SourceFile table
    cursor.execute('''CREATE TABLE IF NOT EXISTS SourceFile (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_file_name TEXT UNIQUE,
                        local_file_path TEXT,
                        file_size INTEGER,
                        status TEXT,
                        created_date TEXT,
                        updated_date TEXT
                    )''')

    # Create ProcessedFiles table
    cursor.execute('''CREATE TABLE IF NOT EXISTS ProcessedFiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        local_file_path TEXT UNIQUE,
                        source_file_name TEXT UNIQUE,
                        status TEXT,
                        created_date TEXT,
                        updated_date TEXT
                    )''')

    conn.commit()
    conn.close()

# Function to insert data into SourceFile table
def insert_source_file(database_name, source_file_name, local_file_path, file_size, status):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    cursor.execute('''INSERT OR IGNORE INTO SourceFile 
                        (source_file_name, local_file_path, file_size, status, created_date, updated_date) 
                        VALUES (?, ?, ?, ?, ?, ?)''',
                    (source_file_name, local_file_path, file_size, status, current_datetime, current_datetime))

    conn.commit()
    conn.close()

# Function to insert data into ProcessedFiles table
def insert_processed_file(database_name, local_file_path, source_file_name, status):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    cursor.execute('''INSERT OR IGNORE INTO ProcessedFiles 
                        (local_file_path, source_file_name, status, created_date, updated_date) 
                        VALUES (?, ?, ?, ?, ?)''',
                    (local_file_path, source_file_name, status, current_datetime, current_datetime))

    conn.commit()
    conn.close()

# Function to update file status in SourceFile table
def update_file_status(database_name, file_name, status):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    cursor.execute('''UPDATE SourceFile SET status = ? WHERE source_file_name = ?''', (status, file_name))

    conn.commit()
    conn.close()

# Function to view database tables
def view_database(database_name, table_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name}')
    data = cursor.fetchall()

    conn.close()
    
    print(f"{table_name} table:")
    if data:
        headers = [description[0] for description in cursor.description]
        print(tabulate(data, headers=headers, tablefmt="grid"))
    else:
        print(f"No data found in {table_name} table.")

def print_database(database_name, table_name):
    view_database(database_name, table_name)
    print()

#Create directories if they don't exist
directories = ['input', 'processing', 'completed', 'failed', 'deleted', 'reports', 'logs']
for directory in directories:
    if not os.path.exists(directory):
        os.makedirs(directory)
# Set up logging
logging.basicConfig(filename='logs/converter.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SSH credentials and server details
hostname = '192.168.1.62'
port = 22
username = 'trellissoft'
password = 'trellissoft@123'
remote_path = '/home/trellissoft/temp_files/'

# Connect to the remote server
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname, port, username, password)

# Function to download folders from remote server
def download_folders_from_remote(sftp, remote_path, local_input_folder, database_name):
    # List files and directories in the remote path
    files = sftp.listdir_attr(remote_path)
    
    for item in files:
        remote_item_path = os.path.join(remote_path, item.filename)
        if stat.S_ISDIR(item.st_mode):  # If it's a directory
            download_folder(sftp, remote_item_path, local_input_folder, database_name)

def download_file(sftp, remote_file_path, local_directory_path, database_name):
    # Get the file name
    file_name = os.path.basename(remote_file_path)
    if file_name.endswith('.wav'):
        # Check if the file has already been downloaded today
        today_date = datetime.now().strftime("%y%m%d")
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM SourceFile WHERE source_file_name = ? AND created_date LIKE ?", (file_name, f"{today_date}%"))
        if cursor.fetchone()[0] > 0:
            # Log that the file has already been downloaded today
            logging.info(f"File '{file_name}' has already been downloaded today. Skipping...")
            conn.close()
            return

        try:
            # Local file path
            local_file_path = os.path.join(local_directory_path, file_name)
            
            # Download the file
            sftp.get(remote_file_path, local_file_path)
            
            logging.info(f"Downloaded '{file_name}' from remote server to local directory.")
            
            # Insert file information into the database
            insert_source_file(database_name, file_name, local_file_path, os.path.getsize(local_file_path), 'pending')
        except Exception as e:
            # Log the error and continue with the next file
            logging.error(f"Error downloading '{file_name}': {str(e)}")
        
        conn.close()
        
# Function to download a folder from remote server
def download_folder(sftp, remote_folder_path, local_input_folder, database_name):
    # Extract the folder name
    folder_name = os.path.basename(remote_folder_path)
    
    # Check if the folder name matches today's date
    today_date = datetime.now().strftime("%y%m%d")
    if folder_name == today_date:
        # Create local directory if it doesn't exist
        local_directory_path = os.path.join(local_input_folder, folder_name)
        os.makedirs(local_directory_path, exist_ok=True)
        
        # List files and directories in the remote folder
        files = sftp.listdir_attr(remote_folder_path)
        
        for item in files:
            remote_item_path = os.path.join(remote_folder_path, item.filename)
            if not stat.S_ISDIR(item.st_mode):  # If it's not a directory
                download_file(sftp, remote_item_path, local_directory_path, database_name)

# Create input folder
input_folder = 'input'
os.makedirs(input_folder, exist_ok=True)

# Create the database and tables if they don't exist
database_name = 'wav_file_manager.db'
create_database(database_name)

# Download folders from remote server to input folder
download_folders_from_remote(ssh.open_sftp(), remote_path, input_folder, database_name)

# Close the SSH connection
ssh.close()

# View the SourceFile table after downloading files
print_database(database_name, "SourceFile")

# Function to convert wav files to mp3 and create chunks
def convert_wav_to_mp3(input_folder, processing_folder, completed_folder, failed_folder, database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Iterate through the input folder
    for root, dirs, files in os.walk(input_folder):
        for file_name in files:
            if file_name.endswith('.wav'):
                # Determine the main folder name (parent folder of the input file)
                main_folder_name = os.path.basename(root)

                # Load the wav file
                wav_file_path = os.path.join(root, file_name)
                try:
                    sound = AudioSegment.from_wav(wav_file_path)
                except CouldntDecodeError:
                    # Move the file to the failed folder
                    failed_main_folder = os.path.join(failed_folder, main_folder_name)
                    os.makedirs(failed_main_folder, exist_ok=True)
                    failed_file_folder = os.path.join(failed_main_folder, file_name[:-4])
                    os.makedirs(failed_file_folder, exist_ok=True)
                    os.rename(wav_file_path, os.path.join(failed_file_folder, file_name))
                    
                    # Update status to 'failed' in the database
                    cursor.execute("UPDATE SourceFile SET status = 'failed' WHERE source_file_name = ?", (file_name,))
                    conn.commit()
                    continue  # Skip further processing for this file

                # Determine the destination folder based on successful processing
                destination_folder = os.path.join(completed_folder, main_folder_name)

                # Create a folder in the completed directory
                os.makedirs(destination_folder, exist_ok=True)

                # Get filename without extension
                filename_without_extension = os.path.splitext(file_name)[0]

                # Create a subfolder inside the main folder for original files
                original_folder = os.path.join(destination_folder, filename_without_extension, "original")
                os.makedirs(original_folder, exist_ok=True)

                # Move the original wav file to the original folder
                original_file_path = os.path.join(original_folder, file_name)
                os.rename(wav_file_path, original_file_path)

                # Insert original file into ProcessedFiles table
                insert_processed_file(database_name, original_file_path, file_name, 'processed')

                # Create a subfolder inside the main folder for converted files
                converted_folder = os.path.join(destination_folder, filename_without_extension, "converted")
                os.makedirs(converted_folder, exist_ok=True)

                # Convert to mp3
                mp3_file_name = f"{filename_without_extension}.mp3"
                mp3_file_path = os.path.join(converted_folder, mp3_file_name)
                sound.export(mp3_file_path, format="mp3")

                # Insert converted file into ProcessedFiles table
                insert_processed_file(database_name, mp3_file_path, mp3_file_name, 'processed')

                # Create a subfolder inside the main folder for chunks
                chunks_folder = os.path.join(destination_folder, filename_without_extension, "chunks")
                os.makedirs(chunks_folder, exist_ok=True)

                # Split the mp3 file into chunks
                for i in range(0, len(sound), 10000):  # Split every 10 seconds
                    start_time = i // 1000  # Convert milliseconds to seconds
                    end_time = (i + 10000) // 1000
                    chunk_file_name = f"{filename_without_extension}_{start_time}-{end_time}.mp3"
                    chunk_file_path = os.path.join(chunks_folder, chunk_file_name)
                    chunk = sound[i:i + 10000]
                    chunk.export(chunk_file_path, format="mp3")

                    # Insert chunk file into ProcessedFiles table
                    insert_processed_file(database_name, chunk_file_path, chunk_file_name, 'processed')

                # Update status to 'completed' in the database for the original file
                cursor.execute("UPDATE SourceFile SET status = 'completed' WHERE source_file_name = ?", (file_name,))
                conn.commit()

    # Close the database connection
    conn.close()



# Set up processing and completed folders
processing_folder = 'processing'
completed_folder = 'completed'
failed_folder = 'failed'
database_name='wav_file_manager.db'

os.makedirs(processing_folder, exist_ok=True)
os.makedirs(completed_folder, exist_ok=True)

# Convert wav files to mp3 and create chunks
convert_wav_to_mp3(input_folder, processing_folder, completed_folder, failed_folder, database_name)

# Move main folders from processing to completed folder and insert data into ProcessedFiles table
def move_main_folders_to_completed(processing_folder, completed_folder, database_name):
    # Get all main folders in the processing directory
    main_folders = [folder for folder in os.listdir(processing_folder) if os.path.isdir(os.path.join(processing_folder, folder))]
    
    # Move each main folder to completed folder
    for main_folder in main_folders:
        processing_main_folder = os.path.join(processing_folder, main_folder)
        completed_main_folder = os.path.join(completed_folder, main_folder)

        # Check if the processing main folder has any files or subfolders
        if any(os.path.isfile(os.path.join(processing_main_folder, file)) or os.path.isdir(os.path.join(processing_main_folder, file)) for file in os.listdir(processing_main_folder)):
            # Move the main folder and its contents
            shutil.move(processing_main_folder, completed_main_folder)

            # Insert processed files into ProcessedFiles table
            for root, dirs, files in os.walk(completed_main_folder):
                for file_name in files:
                    local_file_path = os.path.join(root, file_name)
                    insert_processed_file(database_name, local_file_path, file_name, 'processed')  # Update status to 'completed'

            logging.info(f"Moved '{main_folder}' and its contents to the completed folder.")

# Move main folders from processing to completed folder and insert data into ProcessedFiles table
move_main_folders_to_completed(processing_folder, completed_folder, database_name)

# View the ProcessedFiles table after processing is complete
print_database(database_name, "ProcessedFiles")

# Ensure the processing folder is empty
for root, dirs, files in os.walk(processing_folder, topdown=False):
    for name in files:
        os.remove(os.path.join(root, name))
    for name in dirs:
        os.rmdir(os.path.join(root, name))

logging.info("Processing folder is now empty.")


def create_source_file_report(database_name, report_folder, completed_folder):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Read data from SourceFile table
    df = pd.read_sql_query("SELECT * FROM SourceFile", conn)

    # Iterate through the rows and update status based on folder existence
    for index, row in df.iterrows():
        file_name=row['source_file_name']
        folder_name=file_name[:-4]
        folder_path1 = os.path.join(completed_folder, folder_name)
        folder_path2=os.path.join(failed_folder, folder_name)
        if os.path.exists(folder_path1):
            df.at[index, 'status'] = 'completed'
        elif os.path.exists(folder_path2):
            df.at[index, 'status'] = 'failed'  # Set status to 'failed' if folder doesn't exist

    # Generate report file path
    report_file = os.path.join(report_folder, "SourceFile_Report.xlsx")

    # Write data to Excel file
    df.to_excel(report_file, index=False)

    # Logging
    

    # Close the connection
    conn.close()

    # Generate report file path
    report_file = os.path.join(report_folder, "SourceFile_Report.xlsx")

    # Write data to Excel file
    df.to_excel(report_file, index=False)

    # Logging
    print(f"SourceFile report created and saved as {report_file}")
    logging.info(f"SourceFile report created and saved as {report_file}")

    return report_file


# Function to send email
def send_email(sender_email, sender_password, receiver_email, cc_email, subject, body, attachment_path=None):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Cc'] = cc_email 
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    if attachment_path:
        with open(attachment_path, "rb") as attachment_file:
            attachment=attachment_file.read()  # Read the attachment file
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= " + os.path.basename(attachment_path))
            msg.attach(part)

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)  # Use SSL for Gmail SMTP
    server.login(sender_email, sender_password)
    recipients = [receiver_email]  # Include CC recipient in the list of recipients
    if cc_email:
        recipients.append(cc_email)
    server.sendmail(sender_email, recipients, msg.as_string())  # Send email to both To and CC recipients
    server.quit()


def calculate_file_counts(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Count processed files
    cursor.execute("SELECT COUNT(*) FROM SourceFile WHERE status = 'completed'")
    processed_files = cursor.fetchone()[0]

    # Count failed files
    cursor.execute("SELECT COUNT(*) FROM SourceFile WHERE status = 'failed'")
    failed_files = cursor.fetchone()[0]

    # Count deleted files
    cursor.execute("SELECT COUNT(*) FROM SourceFile WHERE status = 'deleted'")
    deleted_files = cursor.fetchone()[0]


    conn.close()

    return processed_files, failed_files, deleted_files


# Main function
def main():
    # Database details
    database_name = 'wav_file_manager.db'

    # Email details
    sender_email = 'sakshibhati1407@gmail.com'
    sender_password = 'hkcb saoy ymfs wimj'
    receiver_email = 'bsakshi@trellissoft.ai'
    cc_email = 'bsakshi@trellissoft.ai'
    subject = 'Daily Status Report'

    # Create reports folder if it doesn't exist
    report_folder = 'reports'
    os.makedirs(report_folder, exist_ok=True)

    # Create SourceFile report and update all file statuses to 'completed'
    attachment_path = create_source_file_report(database_name, report_folder,completed_folder)

    # Calculate file counts
    processed_files, failed_files, deleted_files = calculate_file_counts(database_name)

    # Prepare email body
    body = f"""Date - {datetime.now().strftime("%d/%m/%Y")}
    Total files - {processed_files + failed_files + deleted_files}
    Processed files - {processed_files}
    Failed files - {failed_files}
    Deleted files - {deleted_files}
    """

    # Send email with attachment
    send_email(sender_email, sender_password, receiver_email,cc_email,subject, body, attachment_path)
if __name__ == "__main__":
    main()