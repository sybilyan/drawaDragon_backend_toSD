import time


def timer_decorator(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        print(f"[{func.__name__}] running after: {end - start} seconds")

        return result

    return wrapper
