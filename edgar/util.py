from functools import wraps

def timeit(f):
    @wraps(f)
    def wrapper(*args, **kw):
        start_time = time.time()
        result = f(*args, **kw)
        end_time = time.time()
        print("{} took {:.2f} seconds."
              .format(f.__name__, end_time-start_time))
        return result
    return wrapper


def write_content(content, output_path):
    """ Writes content to file
    Args:
        content (str)
        output_path (str): path to output file
    """
    with open(output_path, "w", encoding="utf-8") as fout:
        fout.write(content)