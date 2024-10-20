import yaml
from jinja2 import Environment, FileSystemLoader
import os
import logging
import glob

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HOME_DIR = os.environ.get('AIRFLOW_HOME')
SINKS = ['csv2postgre']

def generate_dags(sink: str):
    template_path = os.path.join(HOME_DIR, 'templates')
    output_dir = os.path.join(HOME_DIR, 'dags', 'generated', sink)
    os.makedirs(output_dir, exist_ok=True)

    yaml_paths = glob.glob(os.path.join(HOME_DIR, 'config', sink, '*'))
    for path in yaml_paths:
        with open(path, 'r') as file:
            configs = yaml.safe_load(file)['dags']

        env = Environment(loader=FileSystemLoader(searchpath=template_path))
        template = env.get_template(f'{sink}.jinja')

        for config in configs:
            dag_content = template.render(config)
            dag_filename = f"{config['table']}.py"
            outpath = os.path.join(output_dir, dag_filename)
            with open(outpath, 'w') as dag_file:
                dag_file.write(dag_content)
                logger.info(f'Successfully write DAG file to {outpath}')

        logger.info(f"Generated {len(configs)} DAG files to {output_dir}")

if __name__ == "__main__":
    for sink in SINKS:
        generate_dags(sink)