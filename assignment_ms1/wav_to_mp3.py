import os
import sys
import stat
import time
import shutil
import base64
import sqlite3
import smtplib
import logging
import paramiko
import pandas as pd
from email import encoders
from datetime import datetime
from tabulate import tabulate
from pydub import AudioSegment
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart


# Get the directory path where the script is located
script_directory = os.path.dirname(os.path.realpath(__file__))

# Create directories if they don't exist in the script's directory
directories = ['input', 'processing', 'completed', 'failed', 'deleted', 'reports', 'logs']
for directory in directories:
    directory_path = os.path.join(script_directory, directory)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

# Set up logging
logging.basicConfig(filename='logs/converter.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to create the database and tables if they don't exist
def create_database(database_name):
    try:
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
        logging.info("Table SourceFile created in database.")
        # Create ProcessedFiles table
        cursor.execute('''CREATE TABLE IF NOT EXISTS ProcessedFiles (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            local_file_path TEXT UNIQUE,
                            source_file_name TEXT UNIQUE,
                            status TEXT,
                            created_date TEXT,
                            updated_date TEXT
                        )''')
        
        logging.info("Table ProcessedFiles created in database.")
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error creating database: {e}")
        raise e


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
        logging.info("Data fetched from database...")
    else:
        print(f"No data found in {table_name} table.Exiting...")
        logging.error(f"No data found in {table_name} table.Exiting...")
        sys.exit()

def print_database(database_name, table_name):
    try:
        view_database(database_name, table_name)
        print()
        logging.info(f"Printing database {database_name}...")
    except Exception as e:
        logging.error(f"Error printing database {database_name}: {e}")
        raise e

def base64_decode(encoded_variable):
    decoded_variable = base64.b64decode(encoded_variable.encode()).decode()
    return decoded_variable
    
# SSH credentials and server details
hostname = base64_decode(os.environ.get('SSH_HOST'))
username = base64_decode(os.environ.get('SSH_USERNAME'))
password = base64_decode(os.environ.get('SSH_PASSWORD'))
port = int(base64_decode(os.environ.get('SSH_PORT', '22')))
remote_path="/home/trellissoft/temp_files/"

# Connect to the remote server
try:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, port, username, password)
except Exception as e:
        logging.error(f"Error connecting to server")
        raise e

# Function to download folders from remote server
def download_folders_from_remote(sftp, remote_path, local_input_folder, database_name):
    # List files and directories in the remote 
    try:
        files = sftp.listdir_attr(remote_path)
    except Exception as e:
        print("Hello")
        logging.error(f"Error listing files in remote path '{remote_path}': {str(e)}")
        return
    
    for item in files:
        remote_item_path = os.path.join(remote_path, item.filename)
        if stat.S_ISDIR(item.st_mode):  # If it's a directory
            download_folder(sftp, remote_item_path, local_input_folder, database_name)


def download_file(sftp, remote_file_path, local_directory_path, database_name):
    try:
        # Get the file name
        file_name = os.path.basename(remote_file_path)
        if file_name.endswith('.wav'):
            # Check if the file has already been downloaded today
            today_date = datetime.now().strftime("%y-%m-%d")
            conn = sqlite3.connect(database_name)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM SourceFile WHERE source_file_name = ? AND created_date LIKE ?", (file_name, f"{today_date}%"))
            count = cursor.fetchone()[0]
            if count > 0:
                # Log that the file has already been downloaded today
                logging.info(f"File '{file_name}' has already been downloaded today. Skipping...")
                return 0
            else:
                # Local file path
                local_file_path = os.path.join(local_directory_path, file_name)
                # Download the file
                sftp.get(remote_file_path, local_file_path)
                
                logging.info(f"Downloaded '{file_name}' from remote server to local directory.")
                created_date=datetime.now().strftime("%y-%m-%d")
                cursor.execute("INSERT INTO SourceFile (database_name, source_file_name, local_file_path, file_size, status, created_date) VALUES (?, ?, ?, ?, ?, ?)", 
                                           (database_name, file_name, local_file_path, os.path.getsize(local_file_path), 'pending', created_date))
                return 1
                
    except Exception as e:
        # Log the error and continue with the next file
        logging.error(f"Error downloading '{file_name}': {str(e)}")
    finally:
        conn.close()  # Close the database connection
        
        
   
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
        count=0
        for item in files:
            remote_item_path = os.path.join(remote_folder_path, item.filename)
            if not stat.S_ISDIR(item.st_mode):  # If it's not a directory
                x=download_file(sftp, remote_item_path, local_directory_path, database_name)
                count+=x
        if count==0:
            logging.error("All files have been downloaded today! Exiting...")
            print("All files have been downloaded today! Exiting...")
            sys.exit()        
                
# Create input folder
input_folder = 'input'
os.makedirs(input_folder, exist_ok=True)

# Create the database and tables if they don't exist
database_name = 'wav_file_manager.db'
create_database(database_name)
logging.info("Database created successfully!")

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

                logging.info(f"Processing WAV file: {file_name}")
                
                current_datetime=datetime.now().strftime("%y-%m-%d")
                
                # Load the wav file
                wav_file_path = os.path.join(root, file_name)
                try:
                    sound = AudioSegment.from_wav(wav_file_path)
                except Exception as e:
                    # Log the error and move the file to the failed folder
                    logging.error(f"Error decoding WAV file {wav_file_path}: {e}")
                    failed_main_folder = os.path.join(failed_folder, main_folder_name)
                    os.makedirs(failed_main_folder, exist_ok=True)
                    failed_file_folder = os.path.join(failed_main_folder, file_name[:-4])
                    os.makedirs(failed_file_folder, exist_ok=True)
                    os.rename(wav_file_path, os.path.join(failed_file_folder, file_name))
                    logging.error(f"Moving WAV file {wav_file_path} to failed folder.")
                    
                    updated_date=datetime.now().strftime("%y-%m-%d")
                    # Update status to 'failed'  and date in the database
                    cursor.execute("UPDATE SourceFile SET status = 'failed', updated_date=? WHERE source_file_name = ?", (file_name, updated_date,))
                    conn.commit()
                    logging.error(f"Updating {file_name} status to 'failed' in the database")
                    continue  # Skip further processing for this file 

                # Determine the destination folder based on successful processing
                destination_folder = os.path.join(processing_folder, main_folder_name)

                # Create a folder in the processing directory
                os.makedirs(destination_folder, exist_ok=True)

                # Get filename without extension
                filename_without_extension = os.path.splitext(file_name)[0]

                # Create a subfolder inside the main folder for original files
                original_folder = os.path.join(destination_folder, filename_without_extension, "original")
                os.makedirs(original_folder, exist_ok=True)

                # Move the original wav file to the completed/original folder
                original_file_path = os.path.join(original_folder, file_name)
                try:
                    os.rename(wav_file_path, original_file_path)
                    logging.info(f"Original file {file_name} moved to completed/original.")
                except Exception as e:
                    logging.error(f"Error moving {file_name} to completed/original: {e}")

                # Insert original file into ProcessedFiles table
                try:
                    created_date, updated_date= datetime.now().strftime("%y-%m-%d")
                    cursor.execute("INSERT INTO ProcessedFiles (local_file_path, source_file_name, status, created_date, updated_date) VALUES (?, ?, ?, ?, ?)", 
                                   (original_file_path, file_name, 'processed',current_datetime, current_datetime))
                    conn.commit()
                    logging.info(f"Original file {file_name} inserted to database(ProcessedFiles)")
                except Exception as e:
                    logging.error(f"Error inserting original {file_name} into database: {e}")

                # Create a subfolder inside the main folder for converted files
                converted_folder = os.path.join(destination_folder, filename_without_extension, "converted")
                os.makedirs(converted_folder, exist_ok=True)
                mp3_file_name = f"{filename_without_extension}.mp3"
                mp3_file_path = os.path.join(converted_folder, mp3_file_name)
                try:
                    # Convert to mp3
                    sound.export(mp3_file_path, format="mp3")
                    logging.info(f"Converted file {mp3_file_name} moved to completed/converted.")
                except Exception as e:
                    logging.error(f"Error moving converted {mp3_file_name} to completed/converted: {e}")

                # Insert converted file into ProcessedFiles table
                try:
                    created_date, updated_date= datetime.now().strftime("%y-%m-%d")
                    cursor.execute("INSERT INTO ProcessedFiles (local_file_path, source_file_name, status, created_date, updated_date) VALUES (?, ?, ?, ?, ?)", 
                                   (mp3_file_path, mp3_file_name, 'processed', created_date, updated_date))
                    conn.commit()
                    logging.info(f"Converted file {mp3_file_name} inserted to database(ProcessedFiles)")
                except Exception as e:
                    logging.error(f"Error inserting converted {mp3_file_name} into database: {e}")

                # Create a subfolder inside the main folder for chunks
                chunks_folder = os.path.join(destination_folder, filename_without_extension, "chunks")
                os.makedirs(chunks_folder, exist_ok=True)

                # Split the mp3 file into chunks
                try:
                    for i in range(0, len(sound), 10000):  # Split every 10 seconds
                        start_time = i // 1000  # Convert milliseconds to seconds
                        end_time = (i + 10000) // 1000
                        chunk_file_name = f"{filename_without_extension}_{start_time}-{end_time}.mp3"
                        chunk_file_path = os.path.join(chunks_folder, chunk_file_name)
                        chunk = sound[i:i + 10000]
                        chunk.export(chunk_file_path, format="mp3")
                        logging.info(f"Moving chunk {chunk_file_name} to completed/chunks")
                        # Insert chunk file into ProcessedFiles table
                        try:
                            created_date, updated_date= datetime.now().strftime("%y-%m-%d")
                            cursor.execute("INSERT INTO ProcessedFiles (local_file_path, source_file_name, status, created_date, updated_date) VALUES (?, ?, ?, ?, ?)", 
                                           (chunk_file_path, chunk_file_name, 'processed', created_date, updated_date))
                            conn.commit()
                            logging.info(f"Chunk file {chunk_file_name} inserted into database(ProcessedFiles)")
                        except Exception as e:
                            logging.error(f"Error inserting chunk {chunk_file_name} into database: {e}")
                except Exception as e:
                    logging.error(f"Error splitting mp3 file into chunks: {e}")

                # Update status to 'completed' and date in the database for the original file
                try:
                    updated_date=datetime.now().strftime("%y-%m-%d")
                    cursor.execute("UPDATE SourceFile SET status = 'completed',updated_date=? WHERE source_file_name = ?", (file_name, updated_date, ))
                    logging.info("Status of original file set to completed after the file is processed successfully")
                    conn.commit()
                except Exception as e:
                    logging.error(f"Error updating status of original file to 'completed' after the file is processed: {e}")

    # Close the database connection
    conn.close()

# Set up processing and completed folders
processing_folder = 'processing'
completed_folder = 'completed'
failed_folder = 'failed'
deleted_folder = 'deleted'
database_name = 'wav_file_manager.db'

os.makedirs(processing_folder, exist_ok=True)
os.makedirs(completed_folder, exist_ok=True)

# Convert wav files to mp3 and create chunks
convert_wav_to_mp3(input_folder, processing_folder, completed_folder, failed_folder, database_name)

# Function to move files from processing to completed folder after a delay
def move_files_to_completed_after_delay(processing_folder, completed_folder, delay=1):
    logging.info("Moving file from processing to completed.It may take a few seconds...")
    print("Moving files from processing to completed.It may take a few seconds...")
    time.sleep(delay)  # Wait for the delay period

    for root, dirs, files in os.walk(processing_folder):
        for file in files:
            file_path = os.path.join(root, file)
            destination_path = file_path.replace(processing_folder, completed_folder)
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.move(file_path, destination_path)

# Move files from processing to completed folder after a delay
move_files_to_completed_after_delay(processing_folder, completed_folder)

# View the ProcessedFiles table after processing is complete
print_database(database_name, "ProcessedFiles")


def delete_empty_main_folders(input_folder):
    for item in os.listdir(input_folder):
        item_path = os.path.join(input_folder, item)
        if os.path.isdir(item_path) and not os.listdir(item_path):
            os.rmdir(item_path)

# Delete main empty folders in the input directory
delete_empty_main_folders(input_folder)


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
def send_email(sender_email, sender_password, receiver_email, cc_email, subject, body, attachment_path=None, log_file_path=None):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Cc'] = cc_email 
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    if attachment_path:
        with open(attachment_path, "rb") as attachment_file:
            attachment = attachment_file.read()  # Read the attachment file
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= " + os.path.basename(attachment_path))
            msg.attach(part)

    if log_file_path:
        with open(log_file_path, "rb") as log_file:
            log_attachment = log_file.read()  # Read the log file
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(log_attachment)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= " + os.path.basename(log_file_path))
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
    sender_email = base64_decode(os.environ.get('SENDER_EMAIL'))
    sender_password = base64_decode(os.environ.get('SENDER_PASSWORD'))
    receiver_email = base64_decode(os.environ.get('RECIEVER_EMAIL'))
    cc_email = base64_decode(os.environ.get('CC_EMAIL'))
    subject = 'Daily Status Report'

    # Create reports folder if it doesn't exist
    report_folder = 'reports'
    os.makedirs(report_folder, exist_ok=True)

    # Create SourceFile report and update all file statuses to 'completed'
    attachment_path = create_source_file_report(database_name, report_folder, completed_folder)

    # Get the path to the log file
    log_file_path = 'logs/converter.log'

    # Calculate file counts
    processed_files, failed_files, deleted_files = calculate_file_counts(database_name)

    # Prepare email body
    body = f"""Date - {datetime.now().strftime("%d/%m/%Y")}
    Total files - {processed_files + failed_files + deleted_files}
    Processed files - {processed_files}
    Failed files - {failed_files}
    Deleted files - {deleted_files}
    """

    # Send email with attachment and log file
    send_email(sender_email, sender_password, receiver_email, cc_email, subject, body, attachment_path, log_file_path)

    logging.info("Email sent successfully with the source file report and log file.")
    print("Email sent successfully with the source file report and log file.")

main()
