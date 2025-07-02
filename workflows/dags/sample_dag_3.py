from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param
from workflows.airflow.providers.amazon.aws.operators.sagemaker_workflows \
    import NotebookOperator

###############################################################################
#
# Enter in your desired schedule as WORKFLOW_SCHEDULE.  Some options include:
#
# '@daily' (daily at midnight)
# '@hourly' (every hour, at the top of the hour)
# '30 */3 * * *' (a CRON string, run at minute 30 past every 3rd hour)
# '0 8 * * 1-5' (a CRON string, run every weekday at 8am)
#
###############################################################################

WORKFLOW_SCHEDULE = None

###############################################################################
#
# Enter in the path to your notebook as NOTEBOOK_PATH. Example:
# 'src/workflows/dags/mynotebook.ipynb'
#
###############################################################################

NOTEBOOK_PATH = 'src/<path-to-notebook-file>'

###############################################################################
#
# DAG-level parameters that can be overridden through Airflow UI
#
NotebookOperator.template_fields = ("input_config", "output_config")


DEFAULT_PARAMS = {
    "notebook_path": Param(
        type="string",
        description="The path to the notebook file to execute",
        default=NOTEBOOK_PATH
    ),
    "input_parameters": Param(
        default={},
        type="object",
        description="Parameters passed to the notebook in key=value format"
    )
}
###############################################################################

default_args = {
    'owner': 'shinabel',
}


@dag(
    dag_id='workflow-jg8ck6s',
    default_args=default_args,
    schedule_interval=WORKFLOW_SCHEDULE,
    start_date=datetime(2025, 7, 2),
    is_paused_upon_creation=False,
    tags=['release_center_github', 'shinabel'],
    catchup=False,
    params=DEFAULT_PARAMS
)
def single_notebook():
    def initial_notebook_task():
        notebook1 = NotebookOperator(
            task_id="initial",
            input_config={
                'input_path': "{{params.notebook_path}}",
                'input_params': {
                    "input_parameters_dict": "{{ params.input_parameters }}"
                }
            },
            output_config={
                'output_formats': ['NOTEBOOK']
            },
            wait_for_completion=True,
            poll_interval=5
        )
        return notebook1
    initial_notebook_task()


single_notebook = single_notebook()
