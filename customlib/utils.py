import logging
import sys

def add_custom_logger(name, file_path=None, write_mode='a' , streaming=None, level=logging.INFO):
    '''
    Initiates the logger
    '''

    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    if not len(logger.handlers):
        # Add a filehandler to output to a file
        if file_path:
            file_handler = logging.FileHandler(file_path, mode=write_mode)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # Add a streamhandler to output to console
        if streaming:
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)    

    return logger
