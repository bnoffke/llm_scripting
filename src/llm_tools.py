import os
from openai import OpenAI
import subprocess
import json
import sys

class llm:
    def __init__(self):
        self.MODEL = os.getenv('MODEL')
        llm.client = OpenAI(
        organization=os.getenv('ORG_ID'),
        project=os.getenv('PROJ_ID'),
        )

    def complete(self,instructions,request):
        completion = self.client.chat.completions.create(
            model=self.MODEL,
            messages=[
                {"role": "system", "content": instructions},
                {"role": "user", "content": request}
            ]
            )
        return(completion)
    
class dbt_auto_doc(llm):
    def __init__(self,dbt_model_dir,dbt_model_name):
        self.dbt_project_dir = '/home/bnoffke/stmad_dbt'
        self.dbt_model_dir = f'{self.dbt_project_dir}/{dbt_model_dir}'
        self.dbt_model_name = dbt_model_name
        self.dbt_yml_file = f'{dbt_model_name}.yml'
        self.dbt_yml_filepath = f'{self.dbt_target_dir}/{self.dbt_yml_file}'
        self.universal_md_file = f'{self.dbt_project_dir}/docs/_docs.md'
        if os.path.exists(self.universal_md_file):
            with open(self.universal_md_file, 'r') as file:
                self.universal_md_text = file.read()
        else:
            self.universal_md_text = ''

        if not os.path.exists(self.dbt_yml_filepath):
            self.dbt_yml_text = self.generate_model_yaml()
        else:
            with open(self.dbt_yml_filepath, 'r') as file:
                self.dbt_yml_text = file.read()

    def generate_model_yaml(self) -> str:
        """
        Generate YAML for a single dbt model using codegen
        
        Args:
            model_name: Name of the model to generate YAML for
            dbt_project_dir: Path to dbt project directory
        
        Returns:
            str: The generated YAML content
        """
        command = [
            "dbt",
            "run-operation",
            "generate_model_yaml",
            "--args",
            json.dumps({"model_name": self.model_name})
        ]
        
        try:
            result = subprocess.run(
                command,
                cwd=self.dbt_project_dir,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Error running dbt command: {e}")
            print(f"Error output: {e.stderr}")
            raise

    def generate_descriptions_md(self):
        agent_instruction = """
        You are an expert in munincipal finance, infrastructure, and terminology. You frequently interpret technical column names and provide plain, informative descriptions.

        You will receive a dbt-style yml file for a table that lists columns.
        For each column you need to:
        1. Read the column name
        2. Write a description based on your expertise
        3. Put that description into an output for a .md file using the dbt docs blocks syntax, example:
        yml contents shows "- name: parcel_id"
        You write:
        {% docs parcel_id %}
        This is the identifier for the parcel.
        {% enddocs %}
        Only return the contents of the .md file output.
        """
       
        completion = self.complete(agent_instruction,self.dbt_yml_text)
        md_output = completion.choices[0].message.content.replace('```','').replace('markdown','')
        return(md_output)
    
    def merge_md_file(self,new_md_contents):
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
        completion = self.complete(agent_instruction,request)
        md_output = completion.choices[0].message.content.replace('```','').replace('markdown','')
        with open(self.universal_md_file, 'w+') as file:
            file.write(md_output)
        self.universal_md_text = md_output
        return 
    def update_dbt_yml(self):
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

        my_request = f"""
        <dbt yml>{self.dbt_yml_text}</dbt yml>
        <.md>{self.universal_md_file}</.md>
        """

    def execute_auto_doc(self):
        md_from_yml = self.generate_descriptions_md()
        self.merge_md_file(md_from_yml)
        self.update_dbt_yml()

def __main__():
    dbt_model_dir = sys.argv[0]
    dbt_model_name = sys.argv[1]

    dbt_auto_doccer =  dbt_auto_doc(dbt_model_dir,dbt_model_name)
    dbt_auto_doccer.execute_auto_doc()
