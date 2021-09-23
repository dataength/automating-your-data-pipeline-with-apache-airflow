from airflow.plugins_manager import AirflowPlugin


dataength_link = {
    "name": "Data Eng Thailand",
    "href": "https://www.facebook.com/dataength/",
    "category": "Skooldio Course",
}

airflow_course_link = {
    "name": "Skooldio Course",
    "href": "https://www.skooldio.com/workshops/automating-your-data-piplines-with-apache-airflow",
    "category": "Skooldio Course",
}


class MyAirflowPlugin(AirflowPlugin):
    name = "airflow_course_plugin"
    appbuilder_menu_items = [
        dataength_link,
        airflow_course_link,
    ]
