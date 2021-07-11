from hdfs import Config


class Tools():
    '''This class provides tools for getting decahose data'''

    def __init__(self):
        pass

    def get_file_names(self):
        client = Config().get_client('dev')
        files = client.list('the_dir_path')
        pass
