from airflow.hooks.base_hook import BaseHook


class MyHook(BaseHook):
    def my_method(self, text='...'):
        print(f'Hello, this is {text}. :)')