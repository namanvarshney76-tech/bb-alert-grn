import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import os
import zipfile
from lxml import etree
import tempfile
import warnings
import subprocess
import sys
warnings.filterwarnings("ignore")

# Define the scopes
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

def install_package(package):
    """Install package if not available"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Installed {package}")
    except:
        print(f"Failed to install {package}")

def get_header_row_input():
    """Get header row selection from user"""
    print("\n" + "="*50)
    print("HEADER ROW CONFIGURATION")
    print("="*50)
    print("Please specify where the headers are located in your Excel files:")
    print("  0 = First row (default)")
    print("  1 = Second row")
    print("  2 = Third row")
    print("  etc.")
    print("  -1 = No headers (will create generic column names)")
    
    while True:
        try:
            user_input = input("\nEnter header row number (0 for first row, -1 for no headers): ").strip()
            if user_input == "":
                header_row = 0
                print("Using default: First row (0)")
                break
            
            header_row = int(user_input)
            if header_row >= -1:
                if header_row == -1:
                    print("No headers will be used - generic column names will be created")
                else:
                    print(f"Headers will be read from row {header_row + 1} (index {header_row})")
                break
            else:
                print("Please enter a number >= -1")
                
        except ValueError:
            print("Please enter a valid number")
    
    return header_row

def authenticate():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def get_excel_files(drive_service, folder_id):
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def clean_cell_value(value):
    """Clean and standardize cell values"""
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return ""
        return str(value)
    # Convert to string and remove single quotes
    cleaned = str(value).strip().replace("'", "")
    return cleaned

def clean_dataframe(df):
    """Clean DataFrame by removing rows with blank B column, duplicates, and single quotes"""
    if df.empty:
        return df
    
    print(f"    Original DataFrame shape: {df.shape}")
    
    # Step 1: Remove single quotes from all string columns and replace with blank
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.replace("'", "", regex=False)
    print(f"    Removed single quotes from {len(string_columns)} columns")
    
    # Step 2: Remove rows where second column (B column) is blank/empty
    if len(df.columns) >= 2:
        second_col = df.columns[1]  # Get the second column name
        # Remove rows where second column is empty, NaN, or contains only whitespace
        mask = ~(
            df[second_col].isna() | 
            (df[second_col].astype(str).str.strip() == "") |
            (df[second_col].astype(str).str.strip() == "nan")
        )
        df = df[mask]
        print(f"    After removing rows with blank second column '{second_col}': {df.shape}")
    else:
        print("    Warning: DataFrame has less than 2 columns, skipping blank B column removal")
    
    # Step 3: Remove duplicate rows
    original_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = original_count - len(df)
    if duplicates_removed > 0:
        print(f"    Removed {duplicates_removed} duplicate rows")
    
    print(f"    Final cleaned DataFrame shape: {df.shape}")
    return df

def try_xlsxwriter_read(file_stream):
    """Try using xlsxwriter's read capabilities via conversion"""
    try:
        # This won't work directly as xlsxwriter is write-only
        # But we can try xlwings or other alternatives
        return pd.DataFrame()
    except:
        return pd.DataFrame()

def try_pyxlsb(file_stream, filename, header_row):
    """Try pyxlsb for .xlsb files or as alternative"""
    try:
        import pyxlsb
        file_stream.seek(0)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsb') as tmp_file:
            tmp_file.write(file_stream.read())
            tmp_file.flush()
            
            if header_row == -1:
                df = pd.read_excel(tmp_file.name, engine='pyxlsb', header=None)
            else:
                df = pd.read_excel(tmp_file.name, engine='pyxlsb', header=header_row)
            os.unlink(tmp_file.name)
            return df
    except ImportError:
        print("    pyxlsb not available, skipping...")
        return pd.DataFrame()
    except Exception as e:
        print(f"    pyxlsb failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_xlwings(file_stream, filename, header_row):
    """Try xlwings if available (Windows/Mac with Excel)"""
    try:
        import xlwings as xw
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            file_stream.seek(0)
            tmp_file.write(file_stream.read())
            tmp_file.flush()
            
            app = xw.App(visible=False)
            wb = app.books.open(tmp_file.name)
            ws = wb.sheets[0]
            
            # Get used range
            used_range = ws.used_range
            if used_range:
                data = used_range.value
                if data and len(data) > header_row + 1:
                    if header_row == -1:
                        # No headers - create generic column names
                        num_cols = len(data[0]) if data else 0
                        headers = [f"Column_{i+1}" for i in range(num_cols)]
                        df = pd.DataFrame(data, columns=headers)
                    else:
                        # Use specified header row
                        headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                        df = pd.DataFrame(data[header_row+1:], columns=headers)
                else:
                    df = pd.DataFrame()
            else:
                df = pd.DataFrame()
            
            wb.close()
            app.quit()
            os.unlink(tmp_file.name)
            return df
            
    except ImportError:
        print("    xlwings not available, skipping...")
        return pd.DataFrame()
    except Exception as e:
        print(f"    xlwings failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_xlrd2(file_stream, header_row):
    """Try xlrd2 as alternative to xlrd"""
    try:
        import xlrd2
        file_stream.seek(0)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            tmp_file.write(file_stream.read())
            tmp_file.flush()
            
            if header_row == -1:
                df = pd.read_excel(tmp_file.name, engine='xlrd2', header=None)
            else:
                df = pd.read_excel(tmp_file.name, engine='xlrd2', header=header_row)
            os.unlink(tmp_file.name)
            return df
    except ImportError:
        print("    xlrd2 not available, skipping...")
        return pd.DataFrame()
    except Exception as e:
        print(f"    xlrd2 failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_raw_xml_extraction(file_stream, header_row):
    """More aggressive raw XML extraction"""
    try:
        file_stream.seek(0)
        with zipfile.ZipFile(file_stream, 'r') as zip_ref:
            # List all files to see what's available
            file_list = zip_ref.namelist()
            
            # Look for worksheet files
            worksheet_files = [f for f in file_list if 'xl/worksheets/' in f and f.endswith('.xml')]
            if not worksheet_files:
                return pd.DataFrame()
            
            # Try to read the first worksheet
            with zip_ref.open(worksheet_files[0]) as xml_file:
                # Parse as raw XML without namespace handling
                content = xml_file.read().decode('utf-8', errors='ignore')
                
                # Use regex to extract cell values (crude but sometimes works)
                import re
                
                # Find all cell references and values
                cell_pattern = r'<c[^>]*r="([A-Z]+\d+)"[^>]*>.*?<v[^>]*>([^<]*)</v>'
                cells = re.findall(cell_pattern, content, re.DOTALL)
                
                if not cells:
                    # Try simpler pattern
                    value_pattern = r'<v[^>]*>([^<]*)</v>'
                    values = re.findall(value_pattern, content)
                    if values:
                        # Create simple single-column DataFrame
                        if header_row == -1:
                            return pd.DataFrame(values, columns=["Column_1"])
                        else:
                            return pd.DataFrame(values[header_row+1:], columns=[values[header_row] if len(values) > header_row else "Data"])
                    return pd.DataFrame()
                
                # Convert cell references to row/col coordinates
                cell_data = {}
                max_row = 0
                max_col = 0
                
                for cell_ref, value in cells:
                    # Parse cell reference (e.g., "A1" -> row 1, col 1)
                    col_letters = ''.join([c for c in cell_ref if c.isalpha()])
                    row_num = int(''.join([c for c in cell_ref if c.isdigit()]))
                    
                    col_num = 0
                    for c in col_letters:
                        col_num = col_num * 26 + (ord(c) - ord('A') + 1)
                    
                    cell_data[(row_num, col_num)] = clean_cell_value(value)
                    max_row = max(max_row, row_num)
                    max_col = max(max_col, col_num)
                
                if not cell_data:
                    return pd.DataFrame()
                
                # Convert to 2D array
                data = []
                for row in range(1, max_row + 1):
                    row_data = []
                    for col in range(1, max_col + 1):
                        row_data.append(cell_data.get((row, col), ""))
                    if any(cell for cell in row_data):  # Only add non-empty rows
                        data.append(row_data)
                
                if len(data) < max(1, header_row + 2):
                    return pd.DataFrame()
                
                if header_row == -1:
                    # No headers - create generic column names
                    headers = [f"Column_{i+1}" for i in range(len(data[0]))]
                    return pd.DataFrame(data, columns=headers)
                else:
                    # Use specified header row
                    if len(data) > header_row:
                        headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                        return pd.DataFrame(data[header_row+1:], columns=headers)
                    else:
                        return pd.DataFrame()
                
    except Exception as e:
        print(f"    raw XML extraction failed: {str(e)[:50]}...")
        return pd.DataFrame()

def convert_with_libreoffice(file_stream, filename, header_row):
    """Try converting with LibreOffice command line"""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_input:
            file_stream.seek(0)
            tmp_input.write(file_stream.read())
            tmp_input.flush()
            
            # Try to convert to CSV using LibreOffice
            with tempfile.TemporaryDirectory() as tmp_dir:
                result = subprocess.run([
                    'libreoffice', '--headless', '--convert-to', 'csv',
                    '--outdir', tmp_dir, tmp_input.name
                ], capture_output=True, timeout=30)
                
                if result.returncode == 0:
                    csv_file = os.path.join(tmp_dir, os.path.splitext(os.path.basename(tmp_input.name))[0] + '.csv')
                    if os.path.exists(csv_file):
                        if header_row == -1:
                            df = pd.read_csv(csv_file, header=None)
                        else:
                            df = pd.read_csv(csv_file, header=header_row)
                        os.unlink(tmp_input.name)
                        return df
        
        os.unlink(tmp_input.name)
        return pd.DataFrame()
        
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        print(f"    LibreOffice conversion failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_csv_conversion_with_ssconvert(file_stream, filename, header_row):
    """Try using Gnumeric's ssconvert"""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_input:
            file_stream.seek(0)
            tmp_input.write(file_stream.read())
            tmp_input.flush()
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_output:
                result = subprocess.run([
                    'ssconvert', tmp_input.name, tmp_output.name
                ], capture_output=True, timeout=30)
                
                if result.returncode == 0 and os.path.exists(tmp_output.name):
                    if header_row == -1:
                        df = pd.read_csv(tmp_output.name, header=None)
                    else:
                        df = pd.read_csv(tmp_output.name, header=header_row)
                    os.unlink(tmp_input.name)
                    os.unlink(tmp_output.name)
                    return df
        
        try:
            os.unlink(tmp_input.name)
            os.unlink(tmp_output.name)
        except:
            pass
        return pd.DataFrame()
        
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        print(f"    ssconvert failed: {str(e)[:50]}...")
        return pd.DataFrame()

def read_excel_file(drive_service, file_id, filename, header_row):
    """Ultra-robust Excel reader with maximum fallback strategies"""
    # Download file
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    file_stream.seek(0)

    print(f"  Attempting to read {filename}...")
    print(f"  File size: {len(file_stream.getvalue())} bytes")
    print(f"  Header row setting: {header_row if header_row != -1 else 'No headers'}")

    # Strategy 1: Try xlrd for .xls files first
    if filename.lower().endswith('.xls'):
        try:
            file_stream.seek(0)
            if header_row == -1:
                df = pd.read_excel(file_stream, engine="xlrd", header=None)
            else:
                df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
            if not df.empty:
                print(f"  SUCCESS with xlrd")
                df = clean_dataframe(df)  # Clean the data
                return df
        except Exception as e:
            print(f"  xlrd failed: {str(e)[:50]}...")

    # Strategy 2: Try alternative engines
    engines_to_try = []
    
    # Add pyxlsb if available
    try:
        import pyxlsb
        engines_to_try.append('pyxlsb')
    except ImportError:
        pass
    
    # Try each engine
    for engine in engines_to_try:
        try:
            file_stream.seek(0)
            if header_row == -1:
                df = pd.read_excel(file_stream, engine=engine, header=None)
            else:
                df = pd.read_excel(file_stream, engine=engine, header=header_row)
            if not df.empty:
                print(f"  SUCCESS with {engine}")
                df = clean_dataframe(df)  # Clean the data
                return df
        except Exception as e:
            print(f"  {engine} failed: {str(e)[:50]}...")

    # Strategy 3: Try xlwings (if on Windows/Mac with Excel installed)
    df = try_xlwings(file_stream, filename, header_row)
    if not df.empty:
        print(f"  SUCCESS with xlwings")
        df = clean_dataframe(df)  # Clean the data
        return df

    # Strategy 4: Try raw XML extraction (most aggressive)
    df = try_raw_xml_extraction(file_stream, header_row)
    if not df.empty:
        print(f"  SUCCESS with raw XML extraction")
        df = clean_dataframe(df)  # Clean the data
        return df

    # Strategy 5: Try external conversion tools
    df = convert_with_libreoffice(file_stream, filename, header_row)
    if not df.empty:
        print(f"  SUCCESS with LibreOffice conversion")
        df = clean_dataframe(df)  # Clean the data
        return df

    df = try_csv_conversion_with_ssconvert(file_stream, filename, header_row)
    if not df.empty:
        print(f"  SUCCESS with ssconvert")
        df = clean_dataframe(df)  # Clean the data
        return df

    # Strategy 6: Last resort - try to extract any readable text
    try:
        file_stream.seek(0)
        with zipfile.ZipFile(file_stream, 'r') as zip_ref:
            # Look for any XML files with data
            for file_info in zip_ref.filelist:
                if file_info.filename.endswith('.xml'):
                    try:
                        with zip_ref.open(file_info.filename) as xml_file:
                            content = xml_file.read().decode('utf-8', errors='ignore')
                            # Look for any text that might be data
                            import re
                            text_matches = re.findall(r'>([^<]{2,})<', content)
                            if len(text_matches) > 10:  # If we found some text
                                print(f"  Found some text in {file_info.filename}, but cannot structure it properly")
                                break
                    except:
                        continue
    except Exception as e:
        print(f"  Final text extraction failed: {str(e)[:50]}...")

    print(f"  FAILED - All {6} strategies failed for {filename}")
    
    # Print file info for debugging
    file_stream.seek(0)
    first_bytes = file_stream.read(1000)
    print(f"  First 20 bytes (hex): {first_bytes[:20].hex()}")
    
    return pd.DataFrame()

def append_to_sheet(sheets_service, spreadsheet_id, sheet_name, data, append_headers):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1"
        ).execute()
        existing_rows = result.get('values', [])
        start_row = len(existing_rows) + 1 if existing_rows else 1
    except:
        start_row = 1

    clean_data = data.fillna('').astype(str)

    if append_headers:
        values = [clean_data.columns.tolist()] + clean_data.values.tolist()
    else:
        values = clean_data.values.tolist()

    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A{start_row}",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

def main():
    # Configuration
    FOLDER_ID = '1FxYWfDoe29xSif3xJj1NNvEYHxTRLiM9'
    SPREADSHEET_ID = '170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM'
    SHEET_NAME = 'bbalertgrn'

    print("Enhanced Excel Reader v3.0")
    print("Installing additional packages if needed...")
    
    # Try to install additional packages
    packages_to_try = ['pyxlsb', 'xlwings', 'xlrd2']
    for package in packages_to_try:
        try:
            __import__(package)
            print(f"  {package} is available")
        except ImportError:
            print(f"  {package} not available - will skip related strategies")

    # Get header row configuration from user
    header_row = get_header_row_input()

    # Authenticate and create API clients
    creds = authenticate()
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    # Get list of Excel files
    excel_files = get_excel_files(drive_service, FOLDER_ID)
    if not excel_files:
        print("No Excel files found in the specified folder.")
        return

    print(f"\nFound {len(excel_files)} Excel files to process:")
    for file in excel_files:
        print(f"  - {file['name']}")

    print(f"\nProcessing with header row setting: {header_row if header_row != -1 else 'No headers (generic column names)'}")

    is_first_file = True
    successful_files = 0
    failed_files = 0

    # Process each Excel file
    for i, file in enumerate(excel_files, 1):
        print(f"\n[{i}/{len(excel_files)}] Processing: {file['name']}")
        df = read_excel_file(drive_service, file['id'], file['name'], header_row)
        
        if df.empty:
            print(f"  SKIPPED - No data extracted")
            failed_files += 1
            continue
        
        try:
            print(f"  Data shape: {df.shape}")
            print(f"  Columns: {list(df.columns)[:3]}{'...' if len(df.columns) > 3 else ''}")
            
            append_to_sheet(sheets_service, SPREADSHEET_ID, SHEET_NAME, df, is_first_file)
            print(f"  APPENDED to Google Sheet successfully")
            successful_files += 1
            is_first_file = False
        except Exception as e:
            print(f"  FAILED to append to Google Sheet: {str(e)}")
            failed_files += 1

        def remove_duplicates_from_sheet(sheets_service, spreadsheet_id, sheet_name):
                """Remove duplicate rows from the Google Sheet based on Item Code + po_number."""
                try:
                    # Fetch all rows
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!A1:ZZ"
                    ).execute()

                    values = result.get('values', [])
                    if not values:
                        print("Sheet is empty, skipping duplicate removal.")
                        return

                    # First row is header, rest is data
                    headers = values[0]
                    rows = values[1:]

                    # Load into DataFrame
                    df = pd.DataFrame(rows, columns=headers)
                    before = len(df)

                    # Deduplicate only on Item Code + po_number
                    if "Item Code" in df.columns and "po_number" in df.columns:
                        df = df.drop_duplicates(subset=["Item Code", "po_number"], keep="first")
                        after = len(df)
                        removed = before - after
                    else:
                        print("⚠️ Warning: 'Item Code' or 'po_number' column not found, skipping duplicate removal.")
                        removed = 0
                        after = before

                    # ✅ Clear the entire sheet first
                    sheets_service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id,
                        range=sheet_name
                    ).execute()

                    # Write back cleaned data
                    body = {"values": [headers] + df.values.tolist()}
                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!A1",
                        valueInputOption="RAW",
                        body=body
                    ).execute()

                    print(f"Removed {removed} duplicate rows. Final row count: {after}")

                except Exception as e:
                    print(f"Error while removing duplicates: {str(e)}")

    print(f"\n=== FINAL RESULTS ===")
    print(f"Successfully processed: {successful_files} files")
    print(f"Failed to process: {failed_files} files")
    print(f"Header row used: {header_row if header_row != -1 else 'No headers'}")
    
    if failed_files > 0:
        print(f"\nTo improve success rate, consider:")
        print(f"1. Installing LibreOffice: sudo apt-get install libreoffice")
        print(f"2. Installing Gnumeric: sudo apt-get install gnumeric")
        print(f"3. Re-saving problematic files in Excel as .xlsx format")

if __name__ == '__main__':
    main()