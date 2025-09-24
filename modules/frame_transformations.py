from .assessments_endpoints import *
from google.cloud import bigquery
from google.cloud import storage
import logging
import os
import re



def add_in_grade_levels(test_results):
    # Initialize the BigQuery client
    client = bigquery.Client(project='icef-437920')

    # Execute the query
    query_job = client.query('''
    SELECT DISTINCT 
    CAST(student_number AS STRING) AS local_student_id,
    grade_level AS grade_levels
    FROM `icef-437920.views.student_to_teacher`
    ''')

    # Convert the query results to a Pandas DataFrame
    gl_mapping = query_job.result().to_dataframe()

    # Ensure both columns are string type
    test_results['local_student_id'] = test_results['local_student_id'].astype(str)
    gl_mapping['local_student_id'] = gl_mapping['local_student_id'].astype(str)

    try:
        test_results = pd.merge(test_results, gl_mapping, on='local_student_id', how='left')
    except Exception as e:
        logging.error(f'Unable to merge gl_mapping from BQ due to {e}')
    return test_results


def add_in_unit_col(df):

    # Extract 'Unit', 'Interim', or 'Module' with a number and populate the 'unit' column
    df['unit'] = df['title'].str.extract(r'(Interim(?: Assessment)?(?: #?\d+| \d+[A-Z]*|[_ ](?:PT_)?\d+)?|Unit \d+|Final|Module \d+)', expand=False)

    #Exception for the IA title standalone, #Remove the assessment and hastag form the unit columns
    df.loc[df['title'].str.contains(rf'\b{re.escape("IA")}\b', case=False), 'unit'] = 'Interim 1'
    df['unit'] = df['unit'].str.replace(r'Assessment|#', '', regex=True).str.strip()
    df['unit'] = df['unit'].str.replace(r'\s+', ' ', regex=True)
    #Clean up the interims further from PT, underscores, random spaces
    df['unit'] =  df['unit'].apply(lambda x: re.sub(r'Interim[\s_]+(?:PT_)?(\d+)', r'Interim \1', str(x).strip()))

    #single interim value in unit needs to align with others
    df['unit'] = df['unit'].replace('Interim', 'Interim 1')

    #might need changes eventually
    df['unit'] = df['unit'].replace('Final', 'Final 1')

    #manual insertion as it does not comply with the title scheme properly 9/24/26
    df.loc[df['title'].str.contains('The Outsiders - Mid-Unit Novel Test') | df['title'].str.contains('LOTF Mid-Novel Test'), 'unit'] = 'Mid-Unit 1'

    unit_col_sorting = {'Module 1':  '1',
                        'Mid-Unit 1': '1',
                        'Module 2' : '2',
                        'Module 3' : '3',
                        'Interim 1' : '4',
                        'Interim 2': '5',
                        'Final 1': '6',
                        'Unit 1' : '1',
                        'Unit 2': '2',
                        'Unit 3' : '3'}
    
    df['unit_labels'] = df['unit'].map(unit_col_sorting)

    return(df)






#Add in the Curriculum and Unit Columns via string matching from the Assessment Name
def add_in_curriculum_col(df):
    curriculum_dict = {
        #Moreso HS mapping
        'Geometry': 'Geometry',
        'English': 'English',
        'Algebra I': 'Algebra I',
        'Algebra II': 'Algebra II',
        'Algebra 1': 'Algebra I',
        'Algebra 11': 'Algebra II',
        'PreCal': 'Pre-Calculus',
        'Pre Cal': 'Pre-Calculus',
        'Statistics': 'Statistics',
        'Stats': 'Statistics',
        'Biology': 'Biology',
        'Physics': 'Physics',
        'Government': 'Government',

        #More so Elem Mapping. Less defined
        'IM': 'Math',
        'Checkpoint': 'Checkpoint',
        'Science': 'Science',
        'Into Reading': 'ELA',
        'ELA': 'ELA',
        'Math': 'Math',
        'Quantitative': 'Math',
        'Social Studies': 'History',
        'History': 'History'
    }

    # Initialize the Curriculum column with empty strings
    df['curriculum'] = ''

    # Loop through the dictionary to populate the Curriculum column
    for keyword, label in curriculum_dict.items():
        # Use word boundaries only for the 'IM' keyword to match it as a standalone word
        if keyword == 'IM' or keyword == 'Checkpoint':
            df.loc[df['title'].str.contains(rf'\b{re.escape(keyword)}\b', case=False), 'curriculum'] = label
        else:
            df.loc[df['title'].str.contains(keyword, case=False), 'curriculum'] = label

    #Impossible to string match this one - assessment id 115533 (Grade 8 Interim Assessment #1)
    df.loc[df['assessment_id'] == '115533', 'curriculum'] = 'Science'

    # Interim Assessment #1 | Grade 11 | U.S. History
    df.loc[df['assessment_id'] == '141493', 'curriculum'] = 'US History'

    df.loc[df['assessment_id'] == '141492', 'curriculum'] = 'World History'

    #Environmental Science Interim Assessment #1
    df.loc[df['assessment_id'] == '141508', 'curriculum'] = 'Environmental Science'

    #Algebra 1 IA #1 (Sequences + IM) 
    df.loc[df['assessment_id'] == '141441', 'curriculum'] = 'Algebra I'

    #Biology for Freshman, otherwise Anatomy
    df.loc[(df['assessment_id'] == '141506') & (df['grade_levels'] == 9), 'curriculum'] = 'Biology'
    df.loc[(df['assessment_id'] == '141506') & (df['grade_levels'] != 9), 'curriculum'] = 'Anatomy'

    #Added in 9/24/25
    df.loc[df['assessment_id'].isin(['142819', '143028', '161329']), 'curriculum'] = 'ELA'

    return df



def create_test_type_column(frame):
    frame['test_type'] = ''

    frame['test_type'] = frame['title'].apply(
        lambda x: 'checkpoint' if 'checkpoint' in str(x).lower()
        else 'assessment' if 'assessment' in str(x).lower()
        else 'unknown'
    )
    return frame

def apply_manual_changes(test_results_view):

    # Initialize the BigQuery client
    client = bigquery.Client(project='icef-437920')

    # Execute the query
    changes = client.query('''
    SELECT 
    CAST(assessment_id AS STRING) AS assessment_id, 
    * EXCEPT(assessment_id) 
    FROM `icef-437920.illuminate.illuminate_checkpoint_title_issues`
    ''')

    changes = changes.result().to_dataframe()

    # List of columns to update
    columns_to_update = ['test_type', 'curriculum', 'unit', 'title']

    # Function to update column based on dictionary
    def update_column(df, column_name, update_dict):
        try:
            df[column_name] = df['assessment_id'].map(update_dict).fillna(df[column_name])
            logging.info(f'{column_name} has incurred manual changes')
        except Exception as e:
            logging.error(f'Unable to map for column {column_name} due to {e}')


    # Iterate through columns and apply updates
    for column in columns_to_update:
        update_dict = changes.set_index('assessment_id')[column].to_dict()
        update_column(test_results_view, column, update_dict)

    return test_results_view



def extract_unit(title: str) -> str:
    parts = title.split("_")
    # keep everything between the 2nd and the last part
    unit_core = "_".join(parts[2:-1])
    # if last part is CR or MC, append it
    if parts[-1] in ["CR", "MC"]:
        unit_core += f"_{parts[-1]}"
    return unit_core

def add_in_exit_ticket_info(test_results):
    #Mask for "Exit Ticket" rows
    mask = test_results["title"].str.contains("Exit Ticket", na=False)

    # Update only those rows
    test_results.loc[mask, "unit"] = test_results.loc[mask, "title"].apply(extract_unit)
    test_results.loc[mask, "curriculum"] = "ELA"
    test_results.loc[mask, "test_type"] = "exit ticket"
    return test_results


def create_test_results_view(test_results):

    test_results = add_in_grade_levels(test_results) #subject to be changed to reference BQ view
    test_results = add_in_curriculum_col(test_results)
    test_results = add_in_unit_col(test_results)
    test_results = create_test_type_column(test_results)
    test_results = add_in_exit_ticket_info(test_results)
    # test_results_view = apply_manual_changes(test_results)


    #Add in proficiency col, re-order results, and change names
    test_results.loc[:, 'proficiency'] = test_results['performance_band_level'] + ' ' + test_results['performance_band_label'] #add in proficiency column
    test_results['data_source'] = 'illuminate'

    test_results = test_results[['data_source', 'assessment_id', 'date_taken', 'grade_levels', 'local_student_id', 'test_type', 'curriculum', 'unit', 'unit_labels', 'title', 'standard_code', 'percent_correct', 'performance_band_level', 'performance_band_label', 'proficiency', 'mastered', '__count']]

    #need a validation function on the percent_correct coming out to zero. Ensure there are points possible. Or could calc percent here. 

    test_results = test_results.rename(columns={'grade_levels': 'grade',
                                                'percent_correct': 'score'
                                                  })
    #changes occurs in place
    test_results.loc[test_results['grade'] == 'K', 'grade'] = 0
    #For safe measures
    test_results = test_results.drop_duplicates()
    
    return(test_results)


def append_prior_year(prior_year_file_path, frame, prior_year_file_name):

    prior_year_file_path = os.path.join(prior_year_file_path, prior_year_file_name)

    # Check if prior year file exists
    if os.path.exists(prior_year_file_path):
        logging.info(f'Prior year file found at {prior_year_file_path}. Appending data.')
        # Read the prior year file into a DataFrame
        prior_year_frame = pd.read_csv(prior_year_file_path, encoding='ISO-8859-1')
        
        # Append prior year data to the current frame
        combined_frame = pd.concat([prior_year_frame, frame], ignore_index=True)
        
        # Optional: Remove duplicate rows if needed
        combined_frame = combined_frame.drop_duplicates()
        logging.info(f'Prior year data appended successfully.')

        # Convert the column to datetime format
        combined_frame['date_taken'] = pd.to_datetime(combined_frame['date_taken'])

        # If you want to remove the time part and keep only the date
        combined_frame['date_taken'] = combined_frame['date_taken'].dt.date

    else:
        logging.warning(f'Prior year file not found at {prior_year_file_path}. Using current frame only.')
        combined_frame = frame

    return combined_frame



def send_to_local(save_path, frame, frame_name):
        
    if not frame.empty:

        frame.to_csv(os.path.join(save_path, frame_name), index=False)
        logging.info(f'{frame_name} saved to {save_path}')
    else:
        logging.info(f'No data present in {frame_name} file')



def send_to_gcs(bucket_name, save_path, frame, frame_name):
    """
    Uploads a DataFrame as a CSV file to a GCS bucket.

    Args:
        bucket_name (str): The name of the GCS bucket.
        save_path (str): The path within the bucket where the file will be saved.
        frame (pd.DataFrame): The DataFrame to upload.
        frame_name (str): The name of the file to save.
    """
    if not frame.empty:
        # Initialize the GCS client
        client = storage.Client()

        # Create a temporary local file to save the DataFrame
        temp_file_path = os.path.join("/tmp", frame_name)
        frame.to_csv(temp_file_path, index=False)

        try:
            # Get the bucket
            bucket = client.bucket(bucket_name)

            # Define the blob (file path in the bucket)
            blob = bucket.blob(os.path.join(save_path, frame_name))

            # Upload the file to GCS
            blob.upload_from_filename(temp_file_path)
            logging.info(f"{frame_name} uploaded to GCS bucket {bucket_name} at {save_path}/{frame_name}")
        except Exception as e:
            logging.error(f"Failed to upload {frame_name} to GCS bucket {bucket_name}: {e}")
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    else:
        logging.info(f"No data present in {frame_name} file")




# Columns unique to test_results_no_standard: {'version', 'version_label'}
# Columns unique to test_results_standard: {'academic_benchmark_guid', 'standard_code', 'standard_description'}

def bring_together_test_results(test_results_no_standard, test_results_standard):

    df = pd.concat([test_results_standard, test_results_no_standard])
    df['standard_code'] = df['standard_code'].fillna('percent')
    df = df.drop_duplicates()

    return(df)

