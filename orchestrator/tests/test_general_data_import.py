import os.path
from datetime import datetime

from orchestrator.dags import settings
from orchestrator.dags.general_data_import import extract_calendar_lines


class TestExtractCalendarLines:
    INPUT_FILE_NAME = 'RA221126.TXT'

    def test_creating_result_file(self):
        extract_calendar_lines(TestExtractCalendarLines.INPUT_FILE_NAME)
        date_today = datetime.today().strftime("%Y-%m-%d")
        result_file_name = f"calendar{date_today}.txt"
        result_file_path = os.path.join(settings.AIRFLOW_OUT_DIR, result_file_name)
        assert os.path.exists(result_file_path)
