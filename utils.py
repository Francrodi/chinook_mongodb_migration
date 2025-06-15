import time

def time_function(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        end = time.time()
        print(f"Duracion de la ejecucion: {end - start:.4f} segundos para la funcion {func.__name__}")
        return res
    return wrapper