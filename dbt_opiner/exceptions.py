class DbtNodeNotFoundExeption(Exception):
    def __init__(self, message: str = None, *args, **kwargs):
        full_message = f"{message} Try running dbt compile to generate the manifest file, or make sure the file is part of a well formed dbt project."
        super().__init__(full_message, *args, **kwargs)
