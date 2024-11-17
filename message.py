'''

Created by Alexander Goponenko at 4/7/2022

'''

class Message:

    def __init__(self, time, job_id, variety_id, job_start=None, job_end=None, job_nodes=None):
        self.time = time
        self.job_id = job_id
        self.variety_id = variety_id
        self.job_start = job_start
        self.job_end = job_end
        self.job_nodes = job_nodes