from dl_toolbox_runner.log import logger


class Runner(object):
    def __init__(self):
        self.retrieval_batches = []  # list of dicts with keys 'id', 'conf' and 'files'
        # TODO harmonise file naming with mwr_l12l2 retrieval_batches is called retrieval_dict there

    def run(self):
        self.find_files()
        pass

    def find_files(self):
        logger.info('looking for files to process')


if __name__ == '__main__':
    x = Runner()
    x.run()
