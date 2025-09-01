#!/usr/bin/env python3
"""
Streamlit App for BigBasket Automation Workflows
Combines Gmail attachment downloader and Excel GRN processor
"""

import streamlit as st
import os
import json
import base64
import tempfile
import time
import logging
import pandas as pd
import zipfile
import re
import io
import warnings
import subprocess
import sys
import math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from io import StringIO
from lxml import etree

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

warnings.filterwarnings("ignore")

# Configure Streamlit page
st.set_page_config(
    page_title="BigBasket Automation",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

class BigBasketAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    def authenticate_from_secrets(self, progress_bar, status_text):
        """Authenticate using Streamlit secrets with web-based OAuth flow"""
        try:
            status_text.text("Authenticating with Google APIs...")
            progress_bar.progress(10)
            
            # Check for existing token in session state
            if 'oauth_token' in st.session_state:
                try:
                    combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    creds = Credentials.from_authorized_user_info(st.session_state.oauth_token, combined_scopes)
                    if creds and creds.valid:
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        status_text.text("Authentication successful!")
                        return True
                    elif creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        status_text.text("Authentication successful!")
                        return True
                except Exception as e:
                    st.info(f"Cached token invalid, requesting new authentication: {str(e)}")
            
            # Use Streamlit secrets for OAuth
            if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
                creds_data = json.loads(st.secrets["google"]["credentials_json"])
                combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                
                # Configure for web application
                flow = Flow.from_client_config(
                    client_config=creds_data,
                    scopes=combined_scopes,
                    redirect_uri=st.secrets.get("google", {}).get("redirect_uri", "https://bb-alert-grn.streamlit.app/")
                )
                
                # Generate authorization URL
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                # Check for callback code
                query_params = st.query_params
                if "code" in query_params:
                    try:
                        code = query_params["code"]
                        flow.fetch_token(code=code)
                        creds = flow.credentials
                        
                        # Save credentials in session state
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        
                        progress_bar.progress(100)
                        status_text.text("Authentication successful!")
                        
                        # Clear the code from URL
                        st.query_params.clear()
                        return True
                    except Exception as e:
                        st.error(f"Authentication failed: {str(e)}")
                        return False
                else:
                    # Show authorization link
                    st.markdown("### Google Authentication Required")
                    st.markdown(f"[Authorize with Google]({auth_url})")
                    st.info("Click the link above to authorize, you'll be redirected back automatically")
                    st.stop()
            else:
                st.error("Google credentials missing in Streamlit secrets")
                return False
                
        except Exception as e:
            st.error(f"Authentication failed: {str(e)}")
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            return messages
            
        except Exception as e:
            st.error(f"Email search failed: {str(e)}")
            return []
    
    def process_gmail_workflow(self, config: dict, progress_bar, status_text):
        """Process Gmail attachment download workflow"""
        try:
            status_text.text("Starting Gmail workflow...")
            self._log_message("Starting Gmail workflow...")
            
            # Search for emails
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            progress_bar.progress(25)
            self._log_message(f"Gmail search completed. Found {len(emails)} emails")
            
            if not emails:
                self._log_message("No emails found matching criteria")
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(emails)} emails. Processing attachments...")
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments_BigBasket"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                error_msg = "Failed to create base folder in Google Drive"
                self._log_message(f"ERROR: {error_msg}")
                st.error(error_msg)
                return {'success': False, 'processed': 0}
            
            progress_bar.progress(50)
            
            processed_count = 0
            total_attachments = 0
            
            for i, email in enumerate(emails):
                try:
                    status_text.text(f"Processing email {i+1}/{len(emails)}")
                    
                    # Get email details
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    self._log_message(f"Processing email: {subject} from {sender}")
                    
                    # Get full message
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], email_details, config, base_folder_id
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        self._log_message(f"Found {attachment_count} attachments in: {subject}")
                    
                    progress = 50 + (i + 1) / len(emails) * 45
                    progress_bar.progress(int(progress))
                    
                except Exception as e:
                    error_msg = f"Failed to process email {email.get('id', 'unknown')}: {str(e)}"
                    self._log_message(f"ERROR: {error_msg}")
            
            progress_bar.progress(100)
            final_msg = f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails"
            status_text.text(final_msg)
            self._log_message(f"SUCCESS: {final_msg}")
            
            return {'success': True, 'processed': total_attachments}
            
        except Exception as e:
            error_msg = f"Gmail workflow failed: {str(e)}"
            self._log_message(f"ERROR: {error_msg}")
            st.error(error_msg)
            return {'success': False, 'processed': 0}
    
    def process_excel_workflow(self, config: dict, progress_bar, status_text):
        """Process Excel GRN workflow from Drive files"""
        try:
            status_text.text("Starting Excel GRN workflow...")
            self._log_message("Starting Excel GRN workflow...")
            
            # Get Excel files from Drive folder with date filtering and limit
            excel_files = self._get_excel_files_filtered(
                config['excel_folder_id'], 
                config['days_back'], 
                config['max_files']
            )
            
            progress_bar.progress(25)
            self._log_message(f"Found {len(excel_files)} Excel files (filtered by {config['days_back']} days, max {config['max_files']} files)")
            
            if not excel_files:
                msg = "No Excel files found in the specified folder within the date range"
                self._log_message(msg)
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(excel_files)} Excel files. Processing...")
            
            processed_count = 0
            is_first_file = True
            
            # Check if sheet already has headers
            sheet_has_headers = self._check_sheet_headers(config['spreadsheet_id'], config['sheet_name'])
            
            for i, file in enumerate(excel_files):
                try:
                    status_text.text(f"Processing Excel file {i+1}/{len(excel_files)}: {file['name']}")
                    self._log_message(f"Processing: {file['name']}")
                    
                    # Read Excel file with robust parsing
                    df = self._read_excel_file_robust(file['id'], file['name'], config['header_row'])
                    
                    if df.empty:
                        self._log_message(f"SKIPPED - No data extracted from {file['name']}")
                        continue
                    
                    self._log_message(f"Data shape: {df.shape} - Columns: {list(df.columns)[:3]}{'...' if len(df.columns) > 3 else ''}")
                    
                    # Append to Google Sheet
                    append_headers = is_first_file and not sheet_has_headers
                    self._append_to_sheet(
                        config['spreadsheet_id'], 
                        config['sheet_name'], 
                        df, 
                        append_headers
                    )
                    
                    self._log_message(f"APPENDED to Google Sheet successfully: {file['name']}")
                    processed_count += 1
                    is_first_file = False
                    sheet_has_headers = True
                    
                    progress = 25 + (i + 1) / len(excel_files) * 70
                    progress_bar.progress(int(progress))
                    
                except Exception as e:
                    error_msg = f"Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}"
                    self._log_message(f"ERROR: {error_msg}")
            
            # Remove duplicates
            if processed_count > 0:
                status_text.text("Removing duplicates from Google Sheet...")
                self._log_message("Removing duplicates from Google Sheet...")
                self._remove_duplicates_from_sheet(
                    config['spreadsheet_id'], 
                    config['sheet_name']
                )
            
            progress_bar.progress(100)
            final_msg = f"Excel workflow completed! Processed {processed_count} files"
            status_text.text(final_msg)
            self._log_message(f"SUCCESS: {final_msg}")
            
            return {'success': True, 'processed': processed_count}
            
        except Exception as e:
            error_msg = f"Excel workflow failed: {str(e)}"
            self._log_message(f"ERROR: {error_msg}")
            st.error(error_msg)
            return {'success': False, 'processed': 0}
    
    def _log_message(self, message: str):
        """Add timestamped message to logs"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        if 'logs' not in st.session_state:
            st.session_state.logs = []
        
        log_entry = f"[{timestamp}] {message}"
        st.session_state.logs.append(log_entry)
        
        # Keep only last 200 log entries
        if len(st.session_state.logs) > 200:
            st.session_state.logs = st.session_state.logs[-200:]

    
    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive"""
        try:
            # Check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            st.error(f"Failed to create folder {folder_name}: {str(e)}")
            return ""
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean up filenames"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender_info: Dict, config: dict, base_folder_id: str) -> int:
        """Extract Excel attachments from email"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, sender_info, config, base_folder_id
                )
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            # Filter for Excel files only
            if not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                return 0
            
            try:
                # Get attachment data
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Create folder structure
                sender_email = sender_info.get('sender', 'Unknown')
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()
                
                sender_folder_name = self._sanitize_filename(sender_email)
                type_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)
                
                # Upload file
                clean_filename = self._sanitize_filename(filename)
                final_filename = f"{message_id}_{clean_filename}"
                
                file_metadata = {
                    'name': final_filename,
                    'parents': [type_folder_id]
                }
                
                media = MediaIoBaseUpload(
                    io.BytesIO(file_data),
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                self._log_message(f"Uploaded Excel file: {filename}")
                processed_count += 1
                
            except Exception as e:
                self._log_message(f"ERROR processing attachment {filename}: {str(e)}")
        
        return processed_count
    
    def _get_excel_files_filtered(self, folder_id: str, days_back: int, max_files: int) -> List[Dict]:
        """Get Excel files from Drive folder with date filtering and limit"""
        try:
            # Calculate date threshold
            date_threshold = datetime.now() - timedelta(days=days_back)
            date_threshold_str = date_threshold.strftime('%Y-%m-%dT%H:%M:%S')
            
            query = (f"'{folder_id}' in parents and "
                    f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                    f"mimeType='application/vnd.ms-excel') and "
                    f"createdTime > '{date_threshold_str}'")
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, createdTime)",
                orderBy='createdTime desc',
                pageSize=max_files
            ).execute()
            
            files = results.get('files', [])
            return files
            
        except Exception as e:
            st.error(f"Failed to get Excel files: {str(e)}")
            return []
    
    def _read_excel_file_robust(self, file_id: str, filename: str, header_row: int) -> pd.DataFrame:
        """Robust Excel file reader with multiple fallback strategies"""
        try:
            # Download file
            request = self.drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_stream.seek(0)
            self._log_message(f"Attempting to read {filename} (size: {len(file_stream.getvalue())} bytes)")
            
            # Try openpyxl first
            try:
                file_stream.seek(0)
                if header_row == -1:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=None)
                else:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=header_row)
                if not df.empty:
                    self._log_message("SUCCESS with openpyxl")
                    return self._clean_dataframe(df)
            except Exception as e:
                self._log_message(f"openpyxl failed: {str(e)[:50]}...")
            
            # Try xlrd for older files
            if filename.lower().endswith('.xls'):
                try:
                    file_stream.seek(0)
                    if header_row == -1:
                        df = pd.read_excel(file_stream, engine="xlrd", header=None)
                    else:
                        df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
                    if not df.empty:
                        self._log_message("SUCCESS with xlrd")
                        return self._clean_dataframe(df)
                except Exception as e:
                    self._log_message(f"xlrd failed: {str(e)[:50]}...")
            
            # Try raw XML extraction
            df = self._try_raw_xml_extraction(file_stream, header_row)
            if not df.empty:
                self._log_message("SUCCESS with raw XML extraction")
                return self._clean_dataframe(df)
            
            self._log_message(f"FAILED - All strategies failed for {filename}")
            return pd.DataFrame()
            
        except Exception as e:
            self._log_message(f"ERROR reading {filename}: {str(e)}")
            return pd.DataFrame()
    
    def _try_raw_xml_extraction(self, file_stream: io.BytesIO, header_row: int) -> pd.DataFrame:
        """Raw XML extraction for corrupted Excel files"""
        try:
            file_stream.seek(0)
            with zipfile.ZipFile(file_stream, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                shared_strings = {}
                
                # Read shared strings
                shared_strings_file = 'xl/sharedStrings.xml'
                if shared_strings_file in file_list:
                    try:
                        with zip_ref.open(shared_strings_file) as ss_file:
                            ss_content = ss_file.read().decode('utf-8', errors='ignore')
                            string_pattern = r'<t[^>]*>([^<]*)</t>'
                            strings = re.findall(string_pattern, ss_content, re.DOTALL)
                            for i, string_val in enumerate(strings):
                                shared_strings[str(i)] = string_val.strip()
                    except Exception:
                        pass
                
                # Find worksheet
                worksheet_files = [f for f in file_list if 'xl/worksheets/' in f and f.endswith('.xml')]
                if not worksheet_files:
                    return pd.DataFrame()
                
                with zip_ref.open(worksheet_files[0]) as xml_file:
                    content = xml_file.read().decode('utf-8', errors='ignore')
                    cell_pattern = r'<c[^>]*r="([A-Z]+\d+)"[^>]*(?:t="([^"]*)")?[^>]*>(?:.*?<v[^>]*>([^<]*)</v>)?(?:.*?<is><t[^>]*>([^<]*)</t></is>)?'
                    cells = re.findall(cell_pattern, content, re.DOTALL)
                    
                    if not cells:
                        return pd.DataFrame()
                    
                    cell_data = {}
                    max_row = 0
                    max_col = 0
                    
                    for cell_ref, cell_type, v_value, is_value in cells:
                        col_letters = ''.join([c for c in cell_ref if c.isalpha()])
                        row_num = int(''.join([c for c in cell_ref if c.isdigit()]))
                        col_num = 0
                        for c in col_letters:
                            col_num = col_num * 26 + (ord(c) - ord('A') + 1)
                        
                        if is_value:
                            cell_value = is_value.strip()
                        elif cell_type == 's' and v_value:
                            cell_value = shared_strings.get(v_value, v_value)
                        elif v_value:
                            cell_value = v_value.strip()
                        else:
                            cell_value = ""
                        
                        cell_data[(row_num, col_num)] = self._clean_cell_value(cell_value)
                        max_row = max(max_row, row_num)
                        max_col = max(max_col, col_num)
                    
                    if not cell_data:
                        return pd.DataFrame()
                    
                    data = []
                    for row in range(1, max_row + 1):
                        row_data = []
                        for col in range(1, max_col + 1):
                            row_data.append(cell_data.get((row, col), ""))
                        if any(cell for cell in row_data):
                            data.append(row_data)
                    
                    if len(data) < max(1, header_row + 2):
                        return pd.DataFrame()
                    
                    if header_row == -1:
                        headers = [f"Column_{i+1}" for i in range(len(data[0]))]
                        return pd.DataFrame(data, columns=headers)
                    else:
                        if len(data) > header_row:
                            headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                            return pd.DataFrame(data[header_row+1:], columns=headers)
                        else:
                            return pd.DataFrame()
                
        except Exception as e:
            return pd.DataFrame()
    
    def _clean_cell_value(self, value):
        """Clean and standardize cell values"""
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return ""
            return value  # Preserve numeric type
        cleaned = str(value).strip().replace("'", "")
        try:
            if '.' in cleaned:
                return float(cleaned)
            return int(cleaned)
        except ValueError:
            return cleaned
    
    def _clean_dataframe(self, df):
        """Clean DataFrame by removing blank rows and duplicates"""
        if df.empty:
            return df
        
        # Remove single quotes from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)
        
        # Remove rows where fifth column is blank (changed from second to fifth)
        if len(df.columns) >= 5:
            fifth_col = df.columns[4]  # Index 4 for fifth column
            mask = ~(
                df[fifth_col].isna() | 
                (df[fifth_col].astype(str).str.strip() == "") |
                (df[fifth_col].astype(str).str.strip() == "nan")
            )
            df = df[mask]
        
        # Remove duplicate rows
        original_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = original_count - len(df)
        
        return df
    
    def _check_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Check if Google Sheet already has headers"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1"
            ).execute()
            return bool(result.get('values', []))
        except:
            return False
    
    def _append_to_sheet(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, append_headers: bool):
        """Append DataFrame to Google Sheet while preserving number formatting"""
        try:
            # Prepare data - don't convert everything to strings
            clean_data = df.fillna('')
            
            if append_headers:
                values = [clean_data.columns.tolist()] + clean_data.values.tolist()
            else:
                values = clean_data.values.tolist()
            
            if not values:
                return
            
            # Create the request body with proper value input option
            body = {
                'values': values
            }
            
            # Use USER_ENTERED instead of RAW to preserve number formatting
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='USER_ENTERED',  # Changed from 'RAW' to 'USER_ENTERED'
                body=body
            ).execute()
            
        except Exception as e:
            raise Exception(f"Failed to append to sheet: {str(e)}")
            
            # Find the next empty row
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A"
            ).execute()
            existing_rows = result.get('values', [])
            start_row = len(existing_rows) + 1 if existing_rows else 1
            
            # Append data
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A{start_row}",
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            
            self._log_message(f"Appended {len(values)} rows to Google Sheet")
            
        except Exception as e:
            self._log_message(f"ERROR appending to sheet: {str(e)}")
            raise
    
    def _remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str):
        """Remove duplicate rows from Google Sheet"""
        try:
            # Get all data
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:ZZ"
            ).execute()
            
            values = result.get('values', [])
            if len(values) <= 1:  # No data or only headers
                return
            
            # Keep headers separate
            headers = values[0] if values else []
            data_rows = values[1:] if len(values) > 1 else []
            
            if not data_rows:
                return
            
            # Remove duplicates while preserving order
            seen = set()
            unique_rows = []
            duplicates_count = 0
            
            for row in data_rows:
                # Pad row to match headers length
                padded_row = row + [''] * (len(headers) - len(row))
                row_tuple = tuple(padded_row)
                
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    unique_rows.append(padded_row)
                else:
                    duplicates_count += 1
            
            if duplicates_count > 0:
                # Clear sheet and rewrite with unique data
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:ZZ"
                ).execute()
                
                # Write back headers + unique data
                all_data = [headers] + unique_rows
                body = {'values': all_data}
                
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                self._log_message(f"Removed {duplicates_count} duplicate rows from Google Sheet")
            else:
                self._log_message("No duplicates found in Google Sheet")
                
        except Exception as e:
            self._log_message(f"ERROR removing duplicates: {str(e)}")


def main():
    """Main Streamlit application"""
    st.title("🛒 BigBasket Automation Dashboard")
    st.markdown("---")
    
    # Initialize automation
    if 'automation' not in st.session_state:
        st.session_state.automation = BigBasketAutomation()
    
    # Sidebar for authentication
    with st.sidebar:
        st.header("🔐 Authentication")
        
        if st.button("🚀 Authenticate with Google", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            if st.session_state.automation.authenticate_from_secrets(progress_bar, status_text):
                st.success("✅ Authentication successful!")
                st.rerun()
            else:
                st.error("❌ Authentication failed!")
        
        # Show authentication status
        auth_status = "✅ Authenticated" if st.session_state.automation.gmail_service else "❌ Not Authenticated"
        st.write(f"**Status:** {auth_status}")
    
    # Main content area
    if not st.session_state.automation.gmail_service:
        st.warning("⚠️ Please authenticate with Google first using the sidebar.")
        st.info("👈 Click 'Authenticate with Google' in the sidebar to get started.")
        return
    
    # Workflow tabs - Added separate Logs tab
    tab1, tab2, tab3 = st.tabs(["📧 Gmail Workflow", "📊 Excel GRN Workflow", "📋 Activity Logs"])
    
    with tab1:
        st.header("📧 Gmail Attachment Downloader")
        st.markdown("Download Excel attachments from Gmail and organize them in Google Drive")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Fixed sender email (not editable)
            st.text_input("📨 Sender Email", value="alerts@bigbasket.com", disabled=True)
            # Fixed search term (not editable)  
            st.text_input("🔍 Search Keywords", value="GRN", disabled=True)
            
        with col2:
            # Editable days back (up to 365)
            days_back = st.number_input("📅 Days Back", min_value=1, max_value=365, value=7)
            # Editable max results (up to 1000)
            max_results = st.number_input("📊 Max Emails", min_value=1, max_value=1000, value=50)
            
        # Fixed Google Drive folder ID (not editable but visible)
        st.text_input("📁 Google Drive Folder ID", 
                     value="1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt", 
                     disabled=True)
        
        if st.button("🚀 Start Gmail Workflow", type="primary"):
            config = {
                'sender': "alerts@bigbasket.com",
                'search_term': "GRN",
                'days_back': days_back,
                'max_results': max_results,
                'gdrive_folder_id': "1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt"
            }
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            result = st.session_state.automation.process_gmail_workflow(config, progress_bar, status_text)
            
            if result['success']:
                st.success(f"✅ Gmail workflow completed! Downloaded {result['processed']} attachments.")
            else:
                st.error("❌ Gmail workflow failed. Check logs for details.")
    
    with tab2:
        st.header("📊 Excel GRN Data Processor")
        st.markdown("Process Excel files from Google Drive and consolidate data into Google Sheets")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Fixed folder ID (not editable but visible)
            st.text_input("📁 Excel Files Folder ID", 
                         value="1dQnXXscJsHnl9Ue-zcDazGLQuXAxIUQG", 
                         disabled=True)
            # Fixed spreadsheet ID (not editable but visible)
            st.text_input("📋 Target Spreadsheet ID", 
                         value="170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM", 
                         disabled=True)
            
        with col2:
            # Fixed sheet name (not editable but visible)
            st.text_input("📄 Sheet Name", value="bbalertgrn", disabled=True)
            # Fixed header row (not editable but visible)
            st.number_input("📋 Header Row", value=0, disabled=True)
            
        # Editable fields
        col3, col4 = st.columns(2)
        with col3:
            days_back = st.number_input("📅 Process Files From (Days)", min_value=1, max_value=365, value=7)
        with col4:
            max_files = st.number_input("📊 Max Files to Process", min_value=1, max_value=1000, value=50)
        
        if st.button("🚀 Start Excel Workflow", type="primary"):
            config = {
                'excel_folder_id': "1dQnXXscJsHnl9Ue-zcDazGLQuXAxIUQG",
                'spreadsheet_id': "170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM",
                'sheet_name': "bbalertgrn",
                'header_row': 0,
                'days_back': days_back,
                'max_files': max_files
            }
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            result = st.session_state.automation.process_excel_workflow(config, progress_bar, status_text)
            
            if result['success']:
                st.success(f"✅ Excel workflow completed! Processed {result['processed']} files.")
            else:
                st.error("❌ Excel workflow failed. Check logs for details.")
    
    # New separate Logs tab
    with tab3:
        st.header("📋 Activity Logs")
        st.markdown("Monitor real-time activity and workflow progress")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader("Recent Activity")
        with col2:
            if st.button("🗑️ Clear All Logs"):
                st.session_state.logs = []
                st.rerun()
        
        # Display logs in a container with auto-refresh
        log_container = st.container()
        
        with log_container:
            if 'logs' in st.session_state and st.session_state.logs:
                # Show all logs in reverse order (newest first)
                log_text = "\n".join(reversed(st.session_state.logs))
                st.text_area(
                    "Activity Log", 
                    log_text, 
                    height=400, 
                    disabled=True,
                    key="full_logs"
                )
                
                # Show log statistics
                st.markdown("---")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Log Entries", len(st.session_state.logs))
                
                with col2:
                    error_count = sum(1 for log in st.session_state.logs if "ERROR" in log)
                    st.metric("Error Count", error_count)
                
                with col3:
                    success_count = sum(1 for log in st.session_state.logs if "SUCCESS" in log)
                    st.metric("Success Count", success_count)
                    
            else:
                st.info("No activity logs yet. Start a workflow to see logs appear here.")
                st.markdown("---")
                st.markdown("**Logs will show:**")
                st.markdown("- Email processing status")
                st.markdown("- File upload progress") 
                st.markdown("- Excel processing details")
                st.markdown("- Error messages and debugging info")
                st.markdown("- Success confirmations")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "BigBasket Automation Tool | Built with Streamlit"
        "</div>", 
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()

