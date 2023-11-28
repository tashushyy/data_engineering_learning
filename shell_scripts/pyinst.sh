#!/bin/bash
# Download and install python
curl https://www.python.org/ftp/python/3.10.3/python-3.10.3-amd64.exe -o python-installer.exe
python-installer.exe /quiet InstallAllUsers=1 PrependPath=1
# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate.bat
# Install python libraries based on requirements.txt
pip install -r requirements.txt