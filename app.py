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
                    redirect_uri=st.secrets.get("google", {}).get("redirect_uri", "https://bbalertgrn.streamlit.app/")
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
    
    def process_gmail_workflow(self, config: dict, progress_bar, status_text, log_container):
        """Process Gmail attachment download workflow"""
        try:
            status_text.text("Starting Gmail workflow...")
            self._log_message("Starting Gmail workflow...", log_container)
            
            # Search for emails
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            progress_bar.progress(25)
            self._log_message(f"Gmail search completed. Found {len(emails)} emails", log_container)
            
            if not emails:
                self._log_message("No emails found matching criteria", log_container)
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(emails)} emails. Processing attachments...")
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments_BigBasket"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                error_msg = "Failed to create base folder in Google Drive"
                self._log_message(f"ERROR: {error_msg}", log_container)
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
                    
                    self._log_message(f"Processing email: {subject} from {sender}", log_container)
                    
                    # Get full message
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], email_details, config, base_folder_id, log_container
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        self._log_message(f"Found {attachment_count} attachments in: {subject}", log_container)
                    
                    progress = 50 + (i + 1) / len(emails) * 45
                    progress_bar.progress(int(progress))
                    
                except Exception as e:
                    error_msg = f"Failed to process email {email.get('id', 'unknown')}: {str(e)}"
                    self._log_message(f"ERROR: {error_msg}", log_container)
            
            progress_bar.progress(100)
            final_msg = f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails"
            status_text.text(final_msg)
            self._log_message(f"SUCCESS: {final_msg}", log_container)
            
            return {'success': True, 'processed': total_attachments}
            
        except Exception as e:
            error_msg = f"Gmail workflow failed: {str(e)}"
            self._log_message(f"ERROR: {error_msg}", log_container)
            st.error(error_msg)
            return {'success': False, 'processed': 0}
    
    def process_excel_workflow(self, config: dict, progress_bar, status_text, log_container):
        """Process Excel GRN workflow from Drive files"""
        try:
            status_text.text("Starting Excel GRN workflow...")
            self._log_message("Starting Excel GRN workflow...", log_container)
            
            # Get Excel files from Drive folder
            excel_files = self._get_excel_files(config['excel_folder_id'])
            
            progress_bar.progress(25)
            self._log_message(f"Found {len(excel_files)} Excel files", log_container)
            
            if not excel_files:
                msg = "No Excel files found in the specified folder"
                self._log_message(msg, log_container)
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(excel_files)} Excel files. Processing...")
            
            processed_count = 0
            is_first_file = True
            
            # Check if sheet already has headers
            sheet_has_headers = self._check_sheet_headers(config['spreadsheet_id'], config['sheet_name'])
            
            for i, file in enumerate(excel_files):
                try:
                    status_text.text(f"Processing Excel file {i+1}/{len(excel_files)}: {file['name']}")
                    self._log_message(f"Processing: {file['name']}", log_container)
                    
                    # Read Excel file with robust parsing
                    df = self._read_excel_file_robust(file['id'], file['name'], config['header_row'], log_container)
                    
                    if df.empty:
                        self._log_message(f"SKIPPED - No data extracted from {file['name']}", log_container)
                        continue
                    
                    self._log_message(f"Data shape: {df.shape} - Columns: {list(df.columns)[:3]}{'...' if len(df.columns) > 3 else ''}", log_container)
                    
                    # Append to Google Sheet
                    append_headers = is_first_file and not sheet_has_headers
                    self._append_to_sheet(
                        config['spreadsheet_id'], 
                        config['sheet_name'], 
                        df, 
                        append_headers,
                        log_container
                    )
                    
                    self._log_message(f"APPENDED to Google Sheet successfully: {file['name']}", log_container)
                    processed_count += 1
                    is_first_file = False
                    sheet_has_headers = True
                    
                    progress = 25 + (i + 1) / len(excel_files) * 70
                    progress_bar.progress(int(progress))
                    
                except Exception as e:
                    error_msg = f"Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}"
                    self._log_message(f"ERROR: {error_msg}", log_container)
            
            # Remove duplicates
            if processed_count > 0:
                status_text.text("Removing duplicates from Google Sheet...")
                self._log_message("Removing duplicates from Google Sheet...", log_container)
                self._remove_duplicates_from_sheet(
                    config['spreadsheet_id'], 
                    config['sheet_name'],
                    log_container
                )
            
            progress_bar.progress(100)
            final_msg = f"Excel workflow completed! Processed {processed_count} files"
            status_text.text(final_msg)
            self._log_message(f"SUCCESS: {final_msg}", log_container)
            
            return {'success': True, 'processed': processed_count}
            
        except Exception as e:
            error_msg = f"Excel workflow failed: {str(e)}"
            self._log_message(f"ERROR: {error_msg}", log_container)
            st.error(error_msg)
            return {'success': False, 'processed': 0}
    
    def _log_message(self, message: str, log_container):
        """Add timestamped message to log container"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        if 'logs' not in st.session_state:
            st.session_state.logs = []
        
        log_entry = f"[{timestamp}] {message}"
        st.session_state.logs.append(log_entry)
        
        # Keep only last 100 log entries
        if len(st.session_state.logs) > 100:
            st.session_state.logs = st.session_state.logs[-100:]
        
        # Just update the container (don’t create a new widget)
        log_container.text_area(
            "Activity Log",
            value='\n'.join(st.session_state.logs[-20:]),
            height=300
        )

    
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
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender_info: Dict, config: dict, base_folder_id: str, log_container) -> int:
        """Extract Excel attachments from email"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, sender_info, config, base_folder_id, log_container
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
                
                self._log_message(f"Uploaded Excel file: {filename}", log_container)
                processed_count += 1
                
            except Exception as e:
                self._log_message(f"ERROR processing attachment {filename}: {str(e)}", log_container)
        
        return processed_count
    
    def _get_excel_files(self, folder_id: str) -> List[Dict]:
        """Get Excel files from Drive folder"""
        try:
            query = (f"'{folder_id}' in parents and "
                    f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                    f"mimeType='application/vnd.ms-excel')")
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)",
                orderBy='createdTime desc'
            ).execute()
            
            files = results.get('files', [])
            return files
            
        except Exception as e:
            st.error(f"Failed to get Excel files: {str(e)}")
            return []
    
    def _read_excel_file_robust(self, file_id: str, filename: str, header_row: int, log_container) -> pd.DataFrame:
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
            self._log_message(f"Attempting to read {filename} (size: {len(file_stream.getvalue())} bytes)", log_container)
            
            # Try openpyxl first
            try:
                file_stream.seek(0)
                if header_row == -1:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=None)
                else:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=header_row)
                if not df.empty:
                    self._log_message("SUCCESS with openpyxl", log_container)
                    return self._clean_dataframe(df)
            except Exception as e:
                self._log_message(f"openpyxl failed: {str(e)[:50]}...", log_container)
            
            # Try xlrd for older files
            if filename.lower().endswith('.xls'):
                try:
                    file_stream.seek(0)
                    if header_row == -1:
                        df = pd.read_excel(file_stream, engine="xlrd", header=None)
                    else:
                        df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
                    if not df.empty:
                        self._log_message("SUCCESS with xlrd", log_container)
                        return self._clean_dataframe(df)
                except Exception as e:
                    self._log_message(f"xlrd failed: {str(e)[:50]}...", log_container)
            
            # Try raw XML extraction
            df = self._try_raw_xml_extraction(file_stream, header_row, log_container)
            if not df.empty:
                self._log_message("SUCCESS with raw XML extraction", log_container)
                return self._clean_dataframe(df)
            
            self._log_message(f"FAILED - All strategies failed for {filename}", log_container)
            return pd.DataFrame()
            
        except Exception as e:
            self._log_message(f"ERROR reading {filename}: {str(e)}", log_container)
            return pd.DataFrame()
    
    def _try_raw_xml_extraction(self, file_stream: io.BytesIO, header_row: int, log_container) -> pd.DataFrame:
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
            return str(value)
        cleaned = str(value).strip().replace("'", "")
        return cleaned
    
    def _clean_dataframe(self, df):
        """Clean DataFrame by removing blank rows and duplicates"""
        if df.empty:
            return df
        
        # Remove single quotes from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)
        
        # Remove rows where second column is blank
        if len(df.columns) >= 2:
            second_col = df.columns[1]
            mask = ~(
                df[second_col].isna() | 
                (df[second_col].astype(str).str.strip() == "") |
                (df[second_col].astype(str).str.strip() == "nan")
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
    
    def _append_to_sheet(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, append_headers: bool, log_container):
        """Append DataFrame to Google Sheet"""
        try:
            # Prepare data
            clean_data = df.fillna('').astype(str)
            
            if append_headers:
                values = [clean_data.columns.tolist()] + clean_data.values.tolist()
            else:
                values = clean_data.values.tolist()
            
            if not values:
                return
            
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
            
            self._log_message(f"Appended {len(values)} rows to Google Sheet", log_container)
            
        except Exception as e:
            self._log_message(f"ERROR appending to sheet: {str(e)}", log_container)
            raise
    
    def _remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str, log_container):
        """Remove duplicates based on Item Code and po_number"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:ZZ"
            ).execute()
            values = result.get('values', [])
            
            if not values:
                self._log_message("Sheet is empty, skipping duplicate removal", log_container)
                return
            
            headers = values[0]
            rows = values[1:]
            df = pd.DataFrame(rows, columns=headers)
            before = len(df)
            
            if "Item Code" in df.columns and "po_number" in df.columns:
                df = df.drop_duplicates(subset=["Item Code", "po_number"], keep="first")
                after = len(df)
                removed = before - after
                
                # Update sheet with deduplicated data
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range=sheet_name
                ).execute()
                
                body = {"values": [headers] + df.values.tolist()}
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption="RAW",
                    body=body
                ).execute()
                
                self._log_message(f"Removed {removed} duplicate rows. Final count: {after}", log_container)
            else:
                self._log_message("Warning: 'Item Code' or 'po_number' columns not found, skipping duplicate removal", log_container)
                
        except Exception as e:
            self._log_message(f"ERROR removing duplicates: {str(e)}", log_container)


def create_streamlit_ui():
    """Create the Streamlit user interface"""
    st.title("🛒 BigBasket Automation")
    st.markdown("### Automated Gmail Attachment Processing & Excel GRN Consolidation")
    
    # Initialize automation object
    if 'automation' not in st.session_state:
        st.session_state.automation = BigBasketAutomation()
    
    # Initialize logs
    if 'logs' not in st.session_state:
        st.session_state.logs = []
    
    # Sidebar for navigation and authentication
    st.sidebar.title("Navigation")
    workflow_choice = st.sidebar.selectbox(
        "Select Workflow",
        ["Gmail to Drive", "Drive to Sheets", "Combined Workflow"]
    )
    
    # Authentication section
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔐 Authentication")
    
    if st.sidebar.button("Authenticate Google APIs", key="auth_button"):
        with st.spinner("Authenticating..."):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            success = st.session_state.automation.authenticate_from_secrets(progress_bar, status_text)
            
            if success:
                st.sidebar.success("✅ Authentication successful!")
                st.session_state.authenticated = True
            else:
                st.sidebar.error("❌ Authentication failed")
                st.session_state.authenticated = False
    
    # Check authentication
    if not st.session_state.get('authenticated', False):
        st.warning("⚠️ Please authenticate with Google APIs first using the sidebar")
        st.stop()
    
    st.sidebar.success("✅ Authenticated")
    
    # Configuration section
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### ⚙️ Configuration")
        
        # User configurable parameters
        config_col1, config_col2 = st.columns(2)
        
        with config_col1:
            days_back = st.number_input(
                "Days Back to Search",
                min_value=1,
                max_value=365,
                value=2,
                help="How many days back to search emails"
            )
        
        with config_col2:
            max_results = st.number_input(
                "Maximum Results",
                min_value=1,
                max_value=1000,
                value=1000,
                help="Maximum number of emails to process"
            )
        
        # Header row configuration
        header_row = st.selectbox(
            "Header Row Position",
            options=[0, 1, 2, -1],
            format_func=lambda x: "First row (0)" if x == 0 else "Second row (1)" if x == 1 else "Third row (2)" if x == 2 else "No headers (-1)",
            help="Row number where headers are located (-1 means no headers)"
        )
        
        # Show hardcoded configurations
        with st.expander("📋 Hardcoded Configuration (View Only)", expanded=False):
            st.markdown("**Gmail Configuration:**")
            st.code("""
Sender: alerts@bigbasket.com
Search Term: grn
Gmail Drive Folder: 1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt
            """)
            
            st.markdown("**Excel Configuration:**")
            st.code("""
Excel Source Folder: 1dQnXXscJsHnl9Ue-zcDazGLQuXAxIUQG
Target Spreadsheet: 170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM
Sheet Name: bbalertgrn
Duplicate Removal: Based on Item Code + po_number
            """)
    
    with col2:
        st.markdown("### 📊 Live Activity Log")
        log_container = st.empty()
        
        # Initialize log display
        if st.session_state.logs:
            log_container.text_area(
                "Activity Log",
                value='\n'.join(st.session_state.logs[-20:]),
                height=300,
                key="initial_log_display"
            )
        else:
            log_container.text_area(
                "Activity Log",
                value="[Ready] Waiting for workflow to start...",
                height=300,
                key="empty_log_display"
            )
    
    # Hardcoded configurations
    gmail_config = {
        'sender': 'alerts@bigbasket.com',
        'search_term': 'grn',
        'days_back': days_back,
        'max_results': max_results,
        'gdrive_folder_id': '1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt'
    }
    
    excel_config = {
        'excel_folder_id': '1dQnXXscJsHnl9Ue-zcDazGLQuXAxIUQG',
        'spreadsheet_id': '170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM',
        'sheet_name': 'bbalertgrn',
        'header_row': header_row
    }
    
    st.markdown("---")
    
    # Workflow execution based on choice
    if workflow_choice == "Gmail to Drive":
        st.markdown("### 📧 Gmail to Drive Workflow")
        st.info("Downloads Excel attachments from Gmail and organizes them in Google Drive")
        
        if st.button("🚀 Start Gmail to Drive", type="primary", key="gmail_workflow"):
            with st.spinner("Processing Gmail to Drive workflow..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                result = st.session_state.automation.process_gmail_workflow(
                    gmail_config, progress_bar, status_text, log_container
                )
                
                if result['success']:
                    st.success(f"Gmail to Drive workflow completed! Processed {result['processed']} attachments")
                else:
                    st.error("Gmail to Drive workflow failed")
    
    elif workflow_choice == "Drive to Sheets":
        st.markdown("### 📊 Drive to Sheets Workflow")
        st.info("Processes Excel files from Drive and consolidates them into Google Sheets")
        
        if st.button("🚀 Start Drive to Sheets", type="primary", key="excel_workflow"):
            with st.spinner("Processing Drive to Sheets workflow..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                result = st.session_state.automation.process_excel_workflow(
                    excel_config, progress_bar, status_text, log_container
                )
                
                if result['success']:
                    st.success(f"Drive to Sheets workflow completed! Processed {result['processed']} files")
                else:
                    st.error("Drive to Sheets workflow failed")
    
    else:  # Combined Workflow
        st.markdown("### 🔄 Combined Workflow")
        st.info("Runs Gmail to Drive first, then automatically processes Excel files to Sheets")
        
        if st.button("🚀 Start Combined Workflow", type="primary", key="combined_workflow"):
            with st.spinner("Processing combined workflow..."):
                overall_progress = st.progress(0)
                status_text = st.empty()
                
                gmail_success = True
                excel_success = True
                gmail_processed = 0
                excel_processed = 0
                
                # Phase 1: Gmail to Drive
                st.session_state.automation._log_message("=== PHASE 1: Gmail to Drive ===", log_container)
                status_text.text("Phase 1: Gmail to Drive...")
                
                gmail_progress = st.progress(0)
                gmail_result = st.session_state.automation.process_gmail_workflow(
                    gmail_config, gmail_progress, status_text, log_container
                )
                gmail_success = gmail_result['success']
                gmail_processed = gmail_result['processed']
                
                overall_progress.progress(50)
                
                if gmail_success:
                    st.success(f"✅ Phase 1 completed! Gmail: {gmail_processed} attachments processed")
                    
                    # Phase 2: Drive to Sheets (automatically after Gmail)
                    st.session_state.automation._log_message("=== PHASE 2: Drive to Sheets ===", log_container)
                    status_text.text("Phase 2: Drive to Sheets...")
                    
                    # Add small delay to ensure files are properly saved
                    time.sleep(2)
                    
                    excel_progress = st.progress(0)
                    excel_result = st.session_state.automation.process_excel_workflow(
                        excel_config, excel_progress, status_text, log_container
                    )
                    excel_success = excel_result['success']
                    excel_processed = excel_result['processed']
                    
                    if excel_success:
                        st.success(f"✅ Phase 2 completed! Excel: {excel_processed} files processed")
                    else:
                        st.error("❌ Phase 2 failed")
                else:
                    st.error("❌ Phase 1 failed")
                
                overall_progress.progress(100)
                status_text.text("Combined workflow completed!")
                
                # Final summary
                st.session_state.automation._log_message("=== WORKFLOW SUMMARY ===", log_container)
                if gmail_success and excel_success:
                    summary = f"🎉 Combined workflow completed successfully!\n📧 Gmail: {gmail_processed} attachments\n📊 Excel: {excel_processed} files"
                    st.success(summary)
                    st.session_state.automation._log_message(f"SUCCESS: {gmail_processed} attachments, {excel_processed} files processed", log_container)
                else:
                    summary = "❌ Combined workflow completed with errors"
                    st.error(summary)
                    st.session_state.automation._log_message("ERROR: Workflow completed with failures", log_container)


def create_help_section():
    """Create help section with instructions"""
    with st.sidebar.expander("📋 Instructions", expanded=False):
        st.markdown("""
        ### Setup Steps:
        1. **Authenticate** with Google APIs using the button above
        2. **Configure** days back and maximum results
        3. **Choose workflow**:
           - Gmail to Drive: Downloads attachments only
           - Drive to Sheets: Processes Excel files only
           - Combined: Runs both workflows in sequence
        4. **Monitor** progress in the activity log
        
        ### Workflow Details:
        - **Gmail to Drive**: Downloads Excel attachments from alerts@bigbasket.com
        - **Drive to Sheets**: Processes Excel files and consolidates to Google Sheets
        - **Combined**: Runs Gmail first, then Excel processing automatically
        """)
    
    with st.sidebar.expander("ℹ️ About", expanded=False):
        st.markdown("""
        **BigBasket Automation v1.0**
        
        This application automates:
        - Gmail attachment downloading from BigBasket
        - Excel file processing and consolidation
        - Google Drive organization
        - Data deduplication in Google Sheets
        
        Built with Streamlit and Google APIs.
        """)
    
    # Clear logs button
    with st.sidebar.expander("🗂️ Log Management", expanded=False):
        if st.button("🧹 Clear Activity Logs"):
            st.session_state.logs = []
            st.sidebar.success("Logs cleared!")
            st.experimental_rerun()
        
        if st.session_state.logs:
            log_count = len(st.session_state.logs)
            st.sidebar.info(f"Current log entries: {log_count}")


def main():
    """Main function to run the Streamlit app"""
    try:
        # Initialize session state
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        
        # Create UI components
        create_streamlit_ui()
        create_help_section()
        
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        st.info("Please refresh the page and try again.")


if __name__ == "__main__":

    main()


