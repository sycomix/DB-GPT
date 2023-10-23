import json
import os
import uuid
from typing import Dict, NamedTuple, List
from decimal import Decimal

from pilot.scene.base_message import (
    HumanMessage,
    ViewMessage,
)
from pilot.scene.base_chat import BaseChat
from pilot.scene.base import ChatScene
from pilot.common.sql_database import Database
from pilot.configs.config import Config
from pilot.common.markdown_text import (
    generate_htm_table,
)
from pilot.scene.chat_dashboard.prompt import prompt
from pilot.scene.chat_dashboard.data_preparation.report_schma import (
    ChartData,
    ReportData,
    ValueItem,
)

CFG = Config()


class ChatDashboard(BaseChat):
    chat_scene: str = ChatScene.ChatDashboard.value()
    report_name: str
    """Number of results to return from the query"""

    def __init__(self, chat_session_id, db_name, user_input, report_name):
        """ """
        super().__init__(
            chat_mode=ChatScene.ChatDashboard,
            chat_session_id=chat_session_id,
            current_user_input=user_input,
        )
        if not db_name:
            raise ValueError(f"{ChatScene.ChatDashboard.value} mode should choose db!")
        self.db_name = db_name
        self.report_name = report_name

        self.database = CFG.LOCAL_DB_MANAGE.get_connect(db_name)
        self.db_connect = self.database.session

        self.top_k: int = 5
        self.dashboard_template = self.__load_dashboard_template(report_name)

    def __load_dashboard_template(self, template_name):
        current_dir = os.getcwd()
        print(current_dir)

        current_dir = os.path.dirname(os.path.abspath(__file__))
        with open(f"{current_dir}/template/{template_name}/dashboard.json", "r") as f:
            data = f.read()
        return json.loads(data)

    def generate_input_values(self):
        try:
            from pilot.summary.db_summary_client import DBSummaryClient
        except ImportError:
            raise ValueError("Could not import DBSummaryClient. ")

        client = DBSummaryClient()
        try:
            table_infos = client.get_similar_tables(
                dbname=self.db_name, query=self.current_user_input, topk=self.top_k
            )
            print("dashboard vector find tables:{}", table_infos)
        except Exception as e:
            print(f"db summary find error!{str(e)}")

        return {
            "input": self.current_user_input,
            "dialect": self.database.dialect,
            "table_info": self.database.table_simple_info(),
            "supported_chat_type": self.dashboard_template["supported_chart_type"]
            # "table_info": client.get_similar_tables(dbname=self.db_name, query=self.current_user_input, topk=self.top_k)
        }

    def do_action(self, prompt_response):
        ### TODO 记录整体信息，处理成功的，和未成功的分开记录处理
        chart_datas: List[ChartData] = []
        for chart_item in prompt_response:
            try:
                field_names, datas = self.database.query_ex(
                    self.db_connect, chart_item.sql
                )
                values: List[ValueItem] = []
                data_map = {}
                field_map = {}
                for index, field_name in enumerate(field_names):
                    data_map[f"{field_name}"] = [row[index] for row in datas]
                    field_map[f"{field_name}"] = (
                        False
                        if not data_map[field_name]
                        else all(
                            isinstance(item, (int, float, Decimal))
                            for item in data_map[field_name]
                        )
                    )
                for field_name in field_names[1:]:
                    if not field_map[field_name]:
                        print("more than 2 non-numeric column")
                    else:
                        for data in datas:
                            value_item = ValueItem(
                                name=data[0],
                                type=field_name,
                                value=data[field_names.index(field_name)],
                            )
                            values.append(value_item)

                chart_datas.append(
                    ChartData(
                        chart_uid=str(uuid.uuid1()),
                        chart_name=chart_item.title,
                        chart_type=chart_item.showcase,
                        chart_desc=chart_item.thoughts,
                        chart_sql=chart_item.sql,
                        column_name=field_names,
                        values=values,
                    )
                )
            except Exception as e:
                # TODO 修复流程
                print(e)

        return ReportData(
            conv_uid=self.chat_session_id,
            template_name=self.report_name,
            template_introduce=None,
            charts=chart_datas,
        )
