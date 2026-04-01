# Create a Airflow DAG to extract all the csv files from a folder, process it and save it to (1) a single excel file (with multiple sheets) using pandas, and (2) a single csv file.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import os
import pandas as pd
import glob
from pathlib import Path

def discover_csv_files(folder_path, **kwargs):
    """
    Discover all CSV files in the specified folder
    """
    print(f"🔍 Discovering CSV files in folder: {folder_path}")
    
    # Check if folder exists
    if not os.path.exists(folder_path):
        print(f"❌ Folder {folder_path} does not exist")
        raise ValueError(f"Folder {folder_path} does not exist")
    
    # Find all CSV files
    csv_pattern = os.path.join(folder_path, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print(f"❌ No CSV files found in folder: {folder_path}")
        raise ValueError(f"No CSV files found in folder: {folder_path}")
    
    print(f"✅ Found {len(csv_files)} CSV files:")
    for i, file in enumerate(sorted(csv_files), 1):
        filename = Path(file).name
        size = os.path.getsize(file)
        print(f"  {i:2d}. {filename} ({size:,} bytes)")
    
    # Store the list of files in XCom for the next task
    return csv_files

def process_csv_files(csv_files, **kwargs):
    """
    Process all CSV files with preprocessing and return file paths with metadata
    """
    print(f"Processing {len(csv_files)} CSV files")
    
    # Handle case where csv_files might be a string representation of a list
    if isinstance(csv_files, str):
        print("csv_files is a string, attempting to parse...")
        try:
            import ast
            csv_files = ast.literal_eval(csv_files)
        except:
            print("Failed to parse csv_files string")
            return []
    
    if not csv_files:
        print("No CSV files provided")
        return []
    
    processed_files = []
    failed_files = []
    
    for i, csv_file in enumerate(csv_files, 1):
        filename = Path(csv_file).name
        print(f"\n--- Processing {i}/{len(csv_files)}: {filename} ---")
        
        try:
            # Read CSV file
            print(f"Reading CSV file: {csv_file}")
            df = pd.read_csv(csv_file)
            print(f"Original shape: {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"Columns: {list(df.columns)}")
            
            # Skip empty dataframes
            if df.empty:
                print(f"⚠️  SKIPPING EMPTY FILE: {filename}")
                failed_files.append((filename, "Empty file"))
                continue
            
            # Check for required columns
            if 'text' not in df.columns:
                print(f"⚠️  NO 'text' COLUMN: {filename}")
                failed_files.append((filename, "No 'text' column"))
                continue
            
            # Get filename without extension for group
            group_name = Path(csv_file).stem
            print(f"Group name: {group_name}")
            
            # Add group and username columns
            df['group'] = group_name
            df['username'] = df.apply(lambda x: 'SpeakerA' if hash(str(x)) % 2 == 0 else 'SpeakerB', axis=1)
            print(f"Added group and username columns")
            
            # Create timestamp from end time and drop start/end columns
            if 'end' in df.columns:
                # Convert end time to pandas timestamp with base date 2025-01-01
                df['timestamp'] = pd.to_datetime('2025-01-01 ' + df['end'].str.replace(',', '.'))
                # Drop start and end columns
                df = df.drop(['start', 'end'], axis=1, errors='ignore')
                print(f"Created timestamp column from 'end' column")
            else:
                print(f"⚠️  No 'end' column found for timestamp creation")
            
            # PREPROCESSING: Remove consecutive duplicate text rows
            original_rows = len(df)
            if 'text' in df.columns:
                # Create a mask to identify consecutive duplicates
                df = df[df['text'] != df['text'].shift(1)]
                removed_rows = original_rows - len(df)
                if removed_rows > 0:
                    print(f"Removed {removed_rows} consecutive duplicate text rows")
            
            # Reorder columns to put group, username, and timestamp before text column
            if 'text' in df.columns:
                # Get all columns except group, username, timestamp, and text
                other_cols = [col for col in df.columns if col not in ['group', 'username', 'timestamp', 'text']]
                # Reorder: other columns, then group, username, timestamp, then text
                df = df[other_cols + ['group', 'username', 'timestamp', 'text']]
                print(f"Reordered columns: {list(df.columns)}")
            else:
                # If no text column, just add group, username, and timestamp at the end
                print(f"⚠️  No 'text' column found for reordering")
            
            # Save processed file to temporary location
            temp_file = csv_file.replace('.csv', '_processed.csv')
            df.to_csv(temp_file, index=False)
            processed_files.append(temp_file)
            print(f"✅ SUCCESS: {filename} -> {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"Saved to: {temp_file}")
            
        except Exception as e:
            print(f"❌ ERROR processing {filename}: {str(e)}")
            failed_files.append((filename, str(e)))
            continue
    
    print(f"\n📊 PROCESSING SUMMARY:")
    print(f"✅ Successfully processed: {len(processed_files)} files")
    print(f"❌ Failed to process: {len(failed_files)} files")
    
    if failed_files:
        print(f"\n❌ FAILED FILES:")
        for filename, reason in failed_files:
            print(f"  - {filename}: {reason}")
    
    # Return processed file paths instead of dataframes
    return processed_files

def create_excel_output(processed_files, output_folder, **kwargs):
    """
    Create Excel file with multiple sheets (one per processed file)
    """
    print("Creating Excel output with multiple sheets")
    print(f"Received processed_files: {processed_files}")
    print(f"Type of processed_files: {type(processed_files)}")
    
    # Handle case where processed_files might be a string representation of a list
    if isinstance(processed_files, str):
        print("processed_files is a string, attempting to parse...")
        try:
            import ast
            processed_files = ast.literal_eval(processed_files)
            print(f"Parsed processed_files: {processed_files}")
        except:
            print("Failed to parse processed_files string")
            return None
    
    if not processed_files:
        print("No processed files to create Excel output")
        return None
    
    print(f"Processing {len(processed_files)} processed files")
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Create Excel file path
    excel_path = os.path.join(output_folder, "processed_chats.xlsx")
    
    sheets_created = 0
    
    # Create Excel writer
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for processed_file in processed_files:
            try:
                # Read processed CSV file
                df = pd.read_csv(processed_file)
                
                # Get group from the dataframe (should be the same for all rows)
                group = df['group'].iloc[0] if 'group' in df.columns else f"sheet_{sheets_created}"
                
                # Clean sheet name (Excel has restrictions on sheet names)
                clean_sheet_name = group[:31]  # Excel sheet name limit
                clean_sheet_name = clean_sheet_name.replace('/', '_').replace('\\', '_')
                
                # Write to Excel sheet
                df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                sheets_created += 1
                print(f"Added sheet '{clean_sheet_name}' with {df.shape[0]} rows")
                
            except Exception as e:
                print(f"Error creating Excel sheet: {str(e)}")
                continue
    
    # Check if any sheets were created
    if sheets_created == 0:
        print("No sheets were created - all CSV files were empty or failed to process")
        # Create a dummy sheet to avoid the "at least one sheet must be visible" error
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            pd.DataFrame({'Message': ['No data available']}).to_excel(writer, sheet_name='No_Data', index=False)
        print("Created Excel file with 'No_Data' sheet")
    else:
        print(f"Excel file created: {excel_path} with {sheets_created} sheets")
    
    return excel_path

def create_combined_csv_output(processed_files, output_folder, **kwargs):
    """
    Create single CSV file combining all processed data
    """
    print("Creating combined CSV output")
    print(f"Received processed_files: {processed_files}")
    print(f"Type of processed_files: {type(processed_files)}")
    
    # Handle case where processed_files might be a string representation of a list
    if isinstance(processed_files, str):
        print("processed_files is a string, attempting to parse...")
        try:
            import ast
            processed_files = ast.literal_eval(processed_files)
            print(f"Parsed processed_files: {processed_files}")
        except:
            print("Failed to parse processed_files string")
            return None
    
    if not processed_files:
        print("No processed files to create combined CSV output")
        return None
    
    print(f"Processing {len(processed_files)} processed files")
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Create CSV file path
    csv_path = os.path.join(output_folder, "combined_chats.csv")
    
    all_dataframes = []
    
    for processed_file in processed_files:
        try:
            # Read processed CSV file
            df = pd.read_csv(processed_file)
            all_dataframes.append(df)
            print(f"Added {processed_file}: {df.shape[0]} rows")
            
        except Exception as e:
            print(f"Error reading {processed_file}: {str(e)}")
            continue
    
    if all_dataframes:
        # Ensure all dataframes have the same columns before concatenation
        all_columns = set()
        for df in all_dataframes:
            all_columns.update(df.columns)
        
        # Add missing columns to each dataframe with NaN values
        for df in all_dataframes:
            for col in all_columns:
                if col not in df.columns:
                    df[col] = None
        
        # Reorder columns consistently across all dataframes
        # Put 'group' column second (after 'index') to avoid it being used as index by clair_api_tester.py
        if 'group' in all_columns and 'index' in all_columns:
            other_cols = sorted([col for col in all_columns if col not in ['index', 'group']])
            column_order = ['index', 'group'] + other_cols
        else:
            column_order = sorted(list(all_columns))
        all_dataframes = [df[column_order] for df in all_dataframes]
        
        # Concatenate all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # CRITICAL FIX: Ensure unique index values for clair_api_tester.py
        # The clair_api_tester.py uses index_col=0, so we need unique index values
        if 'index' in combined_df.columns:
            # Reset the index column to have unique values starting from 0
            combined_df['index'] = range(len(combined_df))
            print(f"✅ Fixed index column: Reset to unique values 0-{len(combined_df)-1}")
        
        # Save to CSV
        combined_df.to_csv(csv_path, index=False)
        
        print(f"Combined CSV file created: {csv_path}")
        print(f"Total rows: {combined_df.shape[0]}, Total columns: {combined_df.shape[1]}")
        
        # Verify unique groups
        if 'group' in combined_df.columns:
            unique_groups = combined_df['group'].nunique()
            print(f"Unique groups in combined CSV: {unique_groups}")
            print(f"Group names: {sorted(combined_df['group'].unique())}")
        
        return csv_path
    else:
        print("No valid data to combine - creating empty CSV file")
        # Create an empty CSV with headers
        pd.DataFrame({'Message': ['No data available']}).to_csv(csv_path, index=False)
        print(f"Created empty CSV file: {csv_path}")
        return csv_path

def cleanup_and_summary(excel_path, csv_path, **kwargs):
    """
    Provide summary of the processing results with detailed statistics
    """
    print("=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    
    # Get processed files from XCom to calculate statistics
    processed_files = kwargs['ti'].xcom_pull(task_ids='process_csv_files')
    
    if processed_files:
        # Calculate statistics by reading the processed files
        total_rows = 0
        messages_per_dialogue = []
        
        for processed_file in processed_files:
            try:
                df = pd.read_csv(processed_file)
                total_rows += len(df)
                messages_per_dialogue.append(len(df))
            except:
                continue
        
        if messages_per_dialogue:
            total_dialogues = len(messages_per_dialogue)
            avg_messages = sum(messages_per_dialogue) / len(messages_per_dialogue)
            std_messages = (sum((x - avg_messages) ** 2 for x in messages_per_dialogue) / len(messages_per_dialogue)) ** 0.5
            
            print(f"📊 DIALOGUE STATISTICS:")
            print(f"  Total dialogues processed: {total_dialogues}")
            print(f"  Total messages: {total_rows}")
            print(f"  Average messages per dialogue: {avg_messages:.1f}")
            print(f"  Standard deviation: {std_messages:.1f}")
            print(f"  Min messages in a dialogue: {min(messages_per_dialogue)}")
            print(f"  Max messages in a dialogue: {max(messages_per_dialogue)}")
            print()
            
            print(f"📋 PER DIALOGUE BREAKDOWN:")
            for processed_file in processed_files:
                try:
                    df = pd.read_csv(processed_file)
                    group = df['group'].iloc[0] if 'group' in df.columns else Path(processed_file).stem
                    print(f"  {group}: {len(df)} messages, {df.shape[1]} columns")
                except:
                    continue
            print()
    
    if excel_path and os.path.exists(excel_path):
        excel_size = os.path.getsize(excel_path) / (1024 * 1024)  # MB
        print(f"📁 OUTPUT FILES:")
        print(f"  ✓ Excel file: {excel_path}")
        print(f"    File size: {excel_size:.2f} MB")
    
    if csv_path and os.path.exists(csv_path):
        csv_size = os.path.getsize(csv_path) / (1024 * 1024)  # MB
        print(f"  ✓ Combined CSV file: {csv_path}")
        print(f"    File size: {csv_size:.2f} MB")
    
    print("=" * 60)
    print("Processing completed successfully!")
    print("=" * 60)

def cleanup_temp_files(**kwargs):
    """
    Clean up temporary processed files
    """
    print("Cleaning up temporary files...")
    
    # Get processed files from XCom
    processed_files = kwargs['ti'].xcom_pull(task_ids='process_csv_files')
    
    # Handle case where processed_files might be a string representation of a list
    if isinstance(processed_files, str):
        print("processed_files is a string, attempting to parse...")
        try:
            import ast
            processed_files = ast.literal_eval(processed_files)
        except:
            print("Failed to parse processed_files string")
            return
    
    if processed_files:
        for temp_file in processed_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    print(f"Removed temporary file: {temp_file}")
            except Exception as e:
                print(f"Error removing {temp_file}: {str(e)}")
    
    print("Cleanup completed!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries to avoid hanging
    # 'retry_delay': timedelta(seconds=10),
}

# Define the DAG
with DAG(
    'prep_chats',
    default_args=default_args,
    description='Process CSV files from a folder and create Excel and combined CSV outputs',
    schedule=None,  # Manual trigger only
    catchup=False,
    params={
        'folder_path': Param(
            '/path/to/csv/folder', 
            type='string', 
            description='Path to the folder containing CSV files to process'
        ),
        'output_folder': Param(
            '/path/to/output/data', 
            type='string', 
            description='Path to the output folder for processed files'
        )
    },
    tags=['csv', 'preprocessing', 'excel', 'pandas']
) as dag:

    # Task 1: Discover CSV files
    discover_task = PythonOperator(
        task_id='discover_csv_files',
        python_callable=discover_csv_files,
        op_args=['{{ params.folder_path }}'],
    )

    # Task 2: Process CSV files
    process_task = PythonOperator(
        task_id='process_csv_files',
        python_callable=process_csv_files,
        op_args=['{{ ti.xcom_pull(task_ids="discover_csv_files", key="return_value") }}'],
    )

    # Task 3: Create Excel output
    excel_task = PythonOperator(
        task_id='create_excel_output',
        python_callable=create_excel_output,
        op_args=['{{ ti.xcom_pull(task_ids="process_csv_files", key="return_value") }}', '{{ params.output_folder }}'],
    )

    # Task 4: Create combined CSV output
    csv_task = PythonOperator(
        task_id='create_combined_csv_output',
        python_callable=create_combined_csv_output,
        op_args=['{{ ti.xcom_pull(task_ids="process_csv_files", key="return_value") }}', '{{ params.output_folder }}'],
    )

    # Task 5: Cleanup and summary
    summary_task = PythonOperator(
        task_id='cleanup_and_summary',
        python_callable=cleanup_and_summary,
        op_args=['{{ ti.xcom_pull(task_ids="create_excel_output") }}', '{{ ti.xcom_pull(task_ids="create_combined_csv_output") }}'],
    )
    
    # Task 6: Clean up temporary files
    cleanup_task = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files,
    )
    
    # Define task dependencies
    discover_task >> process_task >> [excel_task, csv_task] >> summary_task >> cleanup_task
