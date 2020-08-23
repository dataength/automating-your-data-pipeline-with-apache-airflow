from airflow.plugins_manager import AirflowPlugin
from flask_admin.base import MenuLink


dataength_link = MenuLink(
    category='Skooldio Course',
    name='Data Eng Thailand',
    url='https://www.facebook.com/dataength/')

airflow_course_link = MenuLink(
    category='Skooldio Course',
    name='Automating Your Data Pipelines with Apache Airflow',
    url='https://www.skooldio.com/workshops/automating-your-data-piplines-with-apache-airflow?skuCode=W16331-01')


class AirflowTestPlugin(AirflowPlugin):
    name = "AirflowCourseMenuLinks"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = [airflow_course_link, dataength_link]