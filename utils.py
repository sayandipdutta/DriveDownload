import datetime, time
from typing import Callable
from functools import wraps

def measure_time(func: Callable) -> Callable:
    """
    Measure time taken by the Callable.
    -----------------------------------------------------
    args    -> func   => Callable

    returns -> caller => Callable

    -----------------------------------------------------
    Decorator to measure time.
    """
    @wraps(func)
    def caller(*args, **kwargs):
        s_time = time.time()
        result = func(*args, **kwargs)
        t_taken = (time.time() - s_time)
        print(f"{func.__name__} took {t_taken: .2f}s")
        return result
    return caller



def bytes_to_MB(bytes: int) -> float:
    """
    converts bytes to MegaBytes
    ---------------------------
    args   -> bytes => int

    return -> MBytes=> float

    ---------------------------
    """

    return bytes / 2 ** 20

def sec_to_hms(sec: float) -> str:
    """
    converts seconds to human readable hh:mm:ss format
    --------------------------------------------------
    args    -> sec              => float

    returns -> formatted_time   => str

    --------------------------------------------------
    """

    return str(datetime.timedelta(seconds=int(sec)))

def download_info(status, start_time: float) -> str:
    """
    Given status object and a start_time returns current 
    status of the download.
    ---------------------------------------------------
    args    -> status       => status object from MediaIOBasedDownload,
            -> start_time   => float

    returns -> print_string => str

    ---------------------------------------------------
    From status object calculates and returns 
    total_size, total_downloaded, remaining_download, ETA, speed
    """

    total = bytes_to_MB(status.total_size)
    remaining = total * (1 - status.progress())
    curr_time = time.time()

    MB_speed = speed = (total - remaining) / (curr_time - start_time)
    unit = 'MBps'
    if speed < 1:
        speed *= 1024
        unit = 'KBps'
    if unit == 'KBps' and speed < 1:
        speed *= 1024
        unit = 'Bps'

    ETA = sec_to_hms(remaining / MB_speed)
    print_string = ' '.join((
        f" Downloaded: {(total - remaining): .2f} MB / {total: .2f} MB",
        f"Remaining: {remaining: .2f} MB",
        f"ETA: {ETA}, speed: {speed: .3f} {unit}                        "
    ))
    return print_string