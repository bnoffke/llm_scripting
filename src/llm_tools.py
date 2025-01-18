import os
from openai import OpenAI
import subprocess
import json
import sys
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dotenv import load_dotenv
from pathlib import Path
from contextlib import redirect_stdout
import io
import glob


#Since this is for personal use, the dbt project directory is hardcoded for now
DBT_PROJECT_DIR = '/home/bnoffke/repos/stmad_dbt'
# Get the path of the current script
script_dir = Path(__file__).resolve().parent

# Construct the .env file path
env_path = script_dir / '../config/.env'  # Adjust relative path as needed

# Load the .env file
load_dotenv(dotenv_path=env_path)

class llm:
    #This class defines the basics for calling an LLM API (OpenAI only for now)
    def __init__(self):
        self.MODEL = os.getenv('MODEL')
        self.client = OpenAI(
            organization=os.getenv('ORG_ID'),
            project=os.getenv('PROJ_ID'),
            api_key=os.getenv('OPENAI_API_KEY')
            )

    def complete(self,instructions,request):
        """
        Assembles, sends, and receives the completion for a given set of system instructions and request contents.
        """

        completion = self.client.chat.completions.create(
            model=self.MODEL,
            messages=[
                {"role": "system", "content": instructions},
                {"role": "user", "content": request}
            ]
            )
        return(completion)

class dbt_auto_doc:
    #This class defines methods to process the documentation for a given dbt model    
    def __init__(self,dbt_model_dir,dbt_model_name,clean_md = False):
        self.llm = llm()
        self.dbt_project_dir = DBT_PROJECT_DIR
        self.dbt_model_dir = f'{self.dbt_project_dir}/{dbt_model_dir}'
        self.dbt_model_name = dbt_model_name
        self.dbt_yml_file = f'{dbt_model_name}.yml'
        self.dbt_yml_filepath = f'{self.dbt_model_dir}/{self.dbt_yml_file}'

        #The universal .md file is expected to contain the AI generated descriptions for the dbt project
        self.universal_md_file = f'{self.dbt_project_dir}/models/docs/_docs.md'

        self.dbt_venv_path = f'{self.dbt_project_dir}/.venv'
        self.dbt_model_sql_filepath = f'{self.dbt_model_dir}/{self.dbt_model_name}.sql'

        #Check if universal md file exists, if so grab the contents and the currently documented columns
        if os.path.exists(self.universal_md_file):
            with open(self.universal_md_file, 'r') as file:
                self.universal_md_text = file.read()

            self.extract_md_col_names()
        else:
            self.universal_md_text = ''
            self.initial_md_col_names = []

        #Grab contents of the dbt model sql (raw file, not compiled)
        try:
            with open(self.dbt_model_sql_filepath, 'r') as file:
                self.dbt_model_sql = file.read()
        except:
            raise Exception('SQL file not found')

        #Previous iterations resulted in a messy md file (duplicate column names, unterminated docs macros)
        #Not as necessary with recent changes, but there's an option to attempt clean-up
        if clean_md:
            self.clean_md_docs()

        #Use dbt codegen to create the contents of the yml file, regardless if it already exists
        #This helps handle new columns when an existing model is updated
        self.dbt_yml_text = self.generate_model_yaml()
        if os.path.exists(self.dbt_yml_filepath):
            with open(self.dbt_yml_filepath, 'r') as file:
                self.existing_dbt_yml_text = file.read()
        else:
            self.existing_dbt_yml_text = ''


    def generate_model_yaml(self):
        """
        Generate YAML for a model using dbt's CLI
        Current approach captures stdout when dbt command is invoked
        """

        # Change to the dbt project directory
        original_dir = os.getcwd()
        os.chdir(self.dbt_project_dir)
        # Capture stdout
        stdout_capture = io.StringIO()

        try:
            with redirect_stdout(stdout_capture):
                # Initialize the dbt runner
                dbt = dbtRunner()
                
                # Run the generate_model_yaml operation
                results: dbtRunnerResult = dbt.invoke([
                    'run-operation', 
                    'generate_model_yaml', 
                    '--args', 
                    f'{{"model_names": ["{self.dbt_model_name}"]}}'
                ])
                
            yaml_content = stdout_capture.getvalue()

            # Check for success
            if results.success:
                print(f'Generated yaml for {self.dbt_model_name}')
                return yaml_content
            else:
                raise Exception(f"dbt command failed: {results.result}")
        
        finally:
            # Close the StringIO object
            stdout_capture.close()

    def extract_md_col_names(self):
        """
        Given a dbt md file with docs blocks macros, extract the column names with OpenAI API
        """

        agent_instruction = """
        You are very familiar with how md files are created to support dbt docs blocks macros. 
        You will receive the contents of an md file. You need to extract a list of the column names, returning a comma delimited list.

        For example, you will see:
        {% docs column_name %}
        Description
        {% enddocs %}

        {% docs other_column_name %}
        Description
        {% enddocs %}

        You will return:
        column_name,other_column_name

        Only return the comma delimited list.
        """

        completion = self.llm.complete(agent_instruction,self.universal_md_text)
        self.initial_md_col_names = completion.choices[0].message.content.split(',')
        #print(self.initial_md_col_names)
    
    def extract_yml_new_col_names(self):
        """
        Returns the new columns found in a yml file and returns them if they are not found in the universal md file.
        """


        agent_instruction = """
        You are very familiar with how dbt yml files are structured for models. Your job is to retrieve the column names specified in a dbt yml file, returning a comma delimited list.

        For example, you will see:
        - name: column_name
        ...
        - name: other_column_name

        You will return:
        column_name,other_column_name

        Only return the comma delimited list.
        """

        completion = self.llm.complete(agent_instruction,self.dbt_yml_text)
        yml_columns = completion.choices[0].message.content.split(',')
        self.new_yml_columns = [col_name for col_name in yml_columns if col_name not in self.initial_md_col_names]
        print(self.new_yml_columns)

    def clean_md_docs(self):
        """
        This attempts to clean up the universal md file by removing duplicate entries and fixing unterminated docs blocks macros.
        Not as useful with recent improvements on updating the universal md file.
        """


        agent_instruction = """
        You are an expert in dbt configuration. Your job is to ensure that the _docs.md file compiles successfully for dbt. The main issues you will find are:
        1. Duplicate named docs blocks.
        2. Unterminated docs blocks.

        You will receive a dbt-style md file with a series of docs blocks. The structure of the file will be a series of docs blocks macros like this:
        {% docs column_name %}
        Description
        {% enddocs %}

        Follow these instructions:
        1. Read through the list of docs blocks.
            If the docs blocks macro is unterminated, add {% enddocs %} after the description.
        2. If you come across another entry with the same name as a previous entry, remove it, preserving the first entry. The description does not need to match to be a duplicate.
            Removal means removing the docs macro syntax as well.

        Only return the contents of the .md file output.
        """

        completion = self.llm.complete(agent_instruction,self.universal_md_text)
        md_output = completion.choices[0].message.content.replace('```','').replace('markdown','')
        with open(self.universal_md_file, 'w+') as file:
            file.write(md_output)
        self.universal_md_text = md_output
        return(md_output)

    def generate_column_descriptions_md(self):
        """
        Given a list of column names, this generates descriptions contained within docs blocks macro, to be used in the universal markdown file.
        """

        agent_instruction = """
        You are an expert in munincipal finance, infrastructure, and terminology. You frequently interpret technical column names and provide plain, informative descriptions.

        You will receive a comma delimited list of column names.
        For each column you need to:
        1. Read the column name
        2. Write a description based on your expertise
        3. Put that description into an output for a .md file using the dbt docs blocks syntax, example:
        Comma delimited list: column_name,other_column_name
        You write:
        {% docs column_name %}
        Description
        {% enddocs %}

        {% docs other_column_name %}
        Description
        {% enddocs %}

        Only return the contents of the .md file output.
        """

        if len(self.new_yml_columns) > 0:
            completion = self.llm.complete(agent_instruction,','.join(self.new_yml_columns))
            md_output = completion.choices[0].message.content.replace('```','').replace('markdown','')
            self.current_column_descriptions_md = md_output
            print(f'Generated docs blocks md contents for {self.dbt_model_name}')
        else:
            md_output = ''
        
        return(md_output)
    
    #Deprecated
    def _merge_md_file(self,new_md_contents): #Deprecated
        agent_instruction = """
        You are responsible for reading and merging .md files for dbt docs blocks. You are an expert in municipal terminology.

        You will receive the contents of 2 .md files, which will contains the dbt docs blocks syntax for column descriptions.
        Follow these steps:
        1. Read the first .md file and note the names within the {% docs <column name> %}
        2. Read the second .md file  and note the names within the {% docs <column name> %}
        3. Determine which columns from the second .md file do not appear in the first .md file
        4. Add the columns (in the form of their docs block macro) to the end of the contents of the first .md file.
        5. Always prioritize preserving the original contents of the first .md file.
        Only return the contents of the updated .md file.
        """

        request = f"""
        <first .md>{self.universal_md_text}</first .md>
        <second .md>{new_md_contents}</second .md>
        """
        completion = self.llm.complete(agent_instruction,request)
        md_output = completion.choices[0].message.content.replace('```','').replace('markdown','')
        with open(self.universal_md_file, 'w+') as file:
            file.write(md_output)
        self.universal_md_text = md_output
        print('Merged new descriptions into universal md file.')
        return(md_output)
    

    def merge_md_file(self,new_md_contents):
        """
        Docs blocks descriptions have been generated for new columns, this will append those new columns to the end of the universal md file.
        """

        if len(new_md_contents) > 0:
            md_output = f'{self.universal_md_text}\n\n{new_md_contents}'
            with open(self.universal_md_file, 'w+') as file:
                file.write(md_output)
            self.universal_md_text = md_output
            print('Merged new descriptions into universal md file.')
        else:
            md_output = ''
        return(md_output)
    
    def extract_model_description(self):
        agent_instruction = """
        You are an expert in dbt model yaml files. Your task is to extract the model description from the provided yaml text.
        Only return the model description.
        """
       
        completion = self.llm.complete(agent_instruction,self.existing_dbt_yml_text)
        self.dbt_model_description = completion.choices[0].message.content.replace('```','').replace('markdown','')
        print(f'Generated description for {self.dbt_model_name}:\n{self.dbt_model_description}')
        return(self.dbt_model_description)

    def generate_model_description(self):
        """
        Given the SQL of a dbt model, this generates a description for the model.
        """

        agent_instruction = """
        You are an expert  in SQL. You frequently translate SQL syntax into human readable text for documentation purposes. You're familiar with municipal finance, enough to understand why some SQL transformations are needed.

        You will receive a dbt-style SQL file. You will read the SQL file to understand the logic and transformations that occur in the file.
        Write an extremely short description that details the actions of the SQL code. Do not mention columns that are returned, unless there is a notable transformation applied to the column.
        Do not write about the purpose of the data, only the SQL transformations that happen in the code.
        Do not speak speculatively, write this as if you are the author of the SQL. There is no need to reference the name of the model in the description you write. Start the description with "This model..."

        Only return the description that you write.
        """
       
        completion = self.llm.complete(agent_instruction,self.dbt_model_sql)
        self.dbt_model_description = completion.choices[0].message.content.replace('```','').replace('markdown','')
        print(f'Generated description for {self.dbt_model_name}:\n{self.dbt_model_description}')
        return(self.dbt_model_description)

    def update_dbt_yml_col_descriptions(self):
        """
        Given the universal md file and the dbt model yml, this matches up columns between the two and inserts the docs macro into the yml file.
        """

        agent_instruction = """
        Your colleague has just documented column descriptions in a .md file for municipal fiance, infrastructure, and terminology. Your job is to apply the dbt docs blocks macros into the provided yml.

        You will receive the contents of a .md file and the contents of a .yml file for a dbt model. Follow these steps:
        1. Read the .md file and note the names within the {% docs <column name> %}
        2. Read the .yml file and match up the column name from step 1.
        3. Insert a new line into the .yml file for each column match, adding a description line with a call for the docs macro. For example:
        You find {% docs parcel_id %} in the .md file
        You find "- name: parcel_id" in the yml file
        Add or update the description property line with a value of '{{ doc("parcel_id") }}'
            Example: description: '{{ doc("table_events") }}'
        Only return the contents of the yml file.
        """

        request = f"""
        <dbt yml>{self.dbt_yml_text}</dbt yml>
        <.md>{self.universal_md_file}</.md>
        """
        completion = self.llm.complete(agent_instruction,request)
        yml_output = completion.choices[0].message.content.replace('```','').replace('markdown','')
        self.dbt_yml_text = yml_output
        with open(self.dbt_yml_filepath, 'w+') as file:
            file.write(yml_output)
        print(f'Created/Updated {self.dbt_model_name}.yml')
        return(yml_output)
    
    def update_dbt_yml_model_description(self):
        """
        This updates the dbt model yml description property with the generated description, preserving existing descriptions if they exist.
        """

        agent_instruction = """
        Your colleague has just documented a description for a dbt SQL model. Your job is to insert the model description into the provided yml.

        If you read the yaml and see a description for the model exists, do nothing and keep the original contents of the file.

        If you see an empty description or missing description line for the model, proceed.
        You will receive a model description and the contents of a .yml file for a dbt model. Follow these steps:
        1. Locate the top-level description property for the model in the yaml file.
            If it doesn't exist, insert a line for the description property.
        2. Insert the provided description as the contents of the description property.
        
        Only return the contents of the yml file.
        """

        request = f"""
        <dbt yml>{self.dbt_yml_text}</dbt yml>
        <model description>{self.dbt_model_description}</model description>
        """
        completion = self.llm.complete(agent_instruction,request)
        self.dbt_yml_text = completion.choices[0].message.content.replace('```','').replace('yaml','')
        with open(self.dbt_yml_filepath, 'w+') as file:
            file.write(self.dbt_yml_text)
        print(f'Created/Updated {self.dbt_model_name}.yml')
        return(self.dbt_yml_text)

    def execute_auto_doc(self):
        """
        This executes the most common sequence of auto-doc commands.
        """

        if self.existing_dbt_yml_text == '':
            self.generate_model_description()
        else:
            self.extract_model_description()
            #self.generate_model_description()
        self.extract_yml_new_col_names()
        md_from_yml = self.generate_column_descriptions_md()
        self.merge_md_file(md_from_yml)
        self.update_dbt_yml_col_descriptions()
        self.update_dbt_yml_model_description()

def __main__():
    """
    Executes the common sequence for dbt documentation generation:
        Column descriptions, managed via .md file
        Model descriptions
        Writes columns descriptions to the universal .md file and writes/creates the .yml file for each dbt model

    Command line arguments:
        1: Model directory (required)
        2: Model name (optional)

    If only the directory is supplied, this will loop over all models in the directory
    """

    dbt_model_dir = sys.argv[1]
    if len(sys.argv) == 2:
        #Only directory specified, document all models in the directory
        print(f'Generating documentation for all models in {DBT_PROJECT_DIR}/{dbt_model_dir}')
        for filepath in glob.glob(f'{DBT_PROJECT_DIR}/{dbt_model_dir}/*.sql'):
            dbt_model_name=filepath.split('/')[-1].split('.')[0]
            print(f'Processing {dbt_model_name}')
            dbt_auto_doccer =  dbt_auto_doc(dbt_model_dir,dbt_model_name)
            dbt_auto_doccer.execute_auto_doc()
    else:
        #Model name is specified, only document this model
        dbt_model_name = sys.argv[2]
        dbt_auto_doccer =  dbt_auto_doc(dbt_model_dir,dbt_model_name)
        
        dbt_auto_doccer.execute_auto_doc()
    
if __name__ == '__main__':
    __main__()