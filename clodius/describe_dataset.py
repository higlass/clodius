import os
import shortuuid
import time

import_time = time.time()


def describe_dataset(args_raw, args_parsed):
    """
    Create a JSON structure describing how this dataset
    was created. The input is the parsed set of command line arguments used to
    run this program.

    :param args: The arguments used to run the program.
    :return: A JSON file with the arguments as well as some other identifying information.
    """
    return {
        "uuid": shortuuid.uuid(),
        "cwd": os.getcwd(),
        "args_raw": args_raw,
        "command": " ".join(args_raw),
        "args_parsed": vars(args_parsed),
        "import_time": time.strftime(
            "%Y-%m-%d %H:%M:%S %z", time.localtime(import_time)
        ),
        "current_time": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "elapsed_time": round(time.time() - import_time, 2),
    }
