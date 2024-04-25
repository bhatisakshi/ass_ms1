# WAV File Manager
This Python script is designed to manage WAV files, including downloading from a remote server, conversion to MP3 format, creation of chunks, generating reports, and sending email notifications.

## Features
Remote Server Interaction: Connects to a remote server via SSH to download WAV files.
Conversion and Chunking: Converts downloaded WAV files to MP3 format and creates chunks.
Database Management: Stores file metadata in a SQLite database.
Reporting: Generates reports on file status and activity.
Email Notifications: Sends daily status reports via email.
## Usage
### Set Up Environment:
Install required dependencies: paramiko, pandas, tabulate, pydub.
Set up environment variables for SSH credentials and email configuration.
### Run the Script:
python wav_file_manager.py
### View Logs and Reports:
Logs are stored in the logs directory.
Reports are saved in the reports directory.
## Configuration
Ensure that environment variables for SSH and email credentials are correctly set.
Modify the script to adjust settings such as file paths, conversion parameters, and email recipients.
## Dependencies
Python 3.x
paramiko: SSH client library for Python.
pandas: Data manipulation and analysis library.
tabulate: Pretty-print tabular data in Python.
pydub: Manipulate audio with a simple and easy high-level interface.
