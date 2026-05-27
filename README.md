# ZEBRA eIDSR Python Sync Script

A lightweight python file that synchronises data from eIDSR to ZEBRA 

## Project Structure (Example)

    project/
        config/
        .gitignore
        README.md
        auth-template.json
        eidsr-zebra-sync.py
        requirements.txt


## Prerequisites

-   Python 3.10+
-   pip
-   Git
-   Virtual environment tool: venv (recommended)

## 1. Create and Activate Virtual Environment

### UNIX (macOS / Linux)

    python3 -m venv venv
    source venv/bin/activate

### Windows (PowerShell)

    python -m venv venv
    .env\Scripts\Activate

## 2. Install Dependencies

After activating your virtual environment:

### UNIX & Windows

    pip install --upgrade pip
    pip install -r requirements.txt

## 3. Copy server auth files to config folder and provide server and user details
 
    cp auth-template.json config/zebra_auth.json
    cp auth-template.json config/eIDSR_auth.json

## 4. Run Sync

### UNIX & Windows

    python3 eidsr-zebra-sync.py
    python eidsr-zebra-sync.py

Sync results will be displayed in console

## 4. Running Tests (optional)

### UNIX & Windows

    pytest

## 5. Exporting Updated Dependencies

    pip freeze > requirements.txt

## Notes

-   Activate your virtual environment before running commands.
-   Ensure the server name and user authentication details are specified correctly in the corresponding zebra_auth.json and eIDSR_auth.json before running project
-   Keep requirements.txt updated for consistent deployments.
