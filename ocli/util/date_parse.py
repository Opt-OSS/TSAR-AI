import dateparser


def parse_to_utc_string(s:str)->str:
    value = dateparser.parse(s,settings={ 'TO_TIMEZONE': 'UTC'})
    return None if value is None else  value.isoformat(timespec='seconds')