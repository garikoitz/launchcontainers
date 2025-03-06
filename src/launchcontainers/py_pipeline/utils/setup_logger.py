import logging
import os.path as Path

def setup_logger(logger_name,console_handler_level,log_dir,log_filename):
    # instantiate logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        "%(asctime)s (%(name)s):[%(levelname)s] %(module)s - %(funcName)s() - line:%(lineno)d   $ %(message)s ",
        datefmt="%Y-%m-%d %H:%M:%S",
    )    
    # define handler and formatter
    console_handler = logging.StreamHandler()
    
    file_handler_info = (
        logging.FileHandler(Path.join(log_dir,f'{log_filename}_info.log'), mode='w')
    ) 
    file_handler_error = (
        logging.FileHandler(Path.join(log_dir,f'{log_filename}_error.log'), mode='w')
    ) 

    # add formatter to handler
    file_handler_info.setFormatter(formatter)
    file_handler_error.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    file_handler_info.setLevel(logging.INFO)
    file_handler_error.setLevel(logging.ERROR)
    console_handler.setLevel(console_handler_level)
    
    # add handler to logger
    logger.addHandler(console_handler) 
    logger.addHandler(file_handler_info)
    logger.addHandler(file_handler_error)
       

    return logger





def main():
    log_dir='/Users/tiger/Desktop'
    log_filename='test_tlei'
    console_handler_level=logging.CRITICAL
    log = setup_logger(__name__,console_handler_level,log_dir,log_filename)
    log.info("This is the info") #20
    log.debug("This is the debug") #10
    log.warning("This is the warn") # 30
    log.critical("This is the fatal") # 50
    log.error("This is the error") # 40
    return

if __name__=='__main__':
    main()

