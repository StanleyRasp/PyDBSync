import json, datetime, os, pickle

def save_object(obj, filename):
    with open(filename, 'w+b') as output:  # Overwrites any existing file.
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)

def read_object(filename) -> object:
    with open(filename, 'rb') as inp:
        return pickle.load(inp)

def clear_log():
    # time to keep logs in days
    LOG_TTL = 30146962

    today = datetime.date.today()
    logs = os.listdir("log/")
    for log in logs:
        if log[0] == '.': continue

        logtime = log.split(".")[0]
        logtime = datetime.date.fromisoformat(logtime)
        if (today - logtime).days > LOG_TTL:
            os.remove(f"log/{log}")


def write_to_log(text):
    with open(f"log/{datetime.date.today()}", "a") as f:
        f.write(text)


def print_to_console(text):
    print_string = f"({datetime.datetime.now()})\n{text}\n\n"
    print(print_string, end="")

    write_to_log(print_string)


def get_json_from_file(file):
    data = {}
    with open(file, "r") as f:
        data = json.load(f)
    return data


def datetime_to_iso(datetime):
    t = datetime
    return f"{t.year}-{str(t.month).rjust(2, '0')}-{str(t.day).rjust(2, '0')}T{str(t.hour).rjust(2, '0')}:{str(t.minute).rjust(2, '0')}:{str(t.second).rjust(2, '0')}"


col_names = ["kierunek_wiatru", "predkosc_wiatru", "opad_atmosferyczny", "temperatura", "wilgotnosc", "cisnienie"]