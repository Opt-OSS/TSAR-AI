import logging
import re

from geopandas import GeoDataFrame

log = logging.getLogger()


def list_dataframe(df: GeoDataFrame,
                   where: str, sort: list, limit=-1
                   ) -> GeoDataFrame:
    """

    :param df: DataFrame
    :param where: DF Where clause
    :param sort:  DF sort clause in form "+field -field2" where "+" for ascending "-" for descending
    :param limit: Limit output records outout to max [limit] records (-1 for unlimited)
    :return:  filtered DF copy
    """
    _df = df
    # where
    if where:
        try:
            _df = _df.query(where)
        except ValueError as e:
            raise ValueError(f"--where {e}")
    # columns
    if sort:
        # soring
        pattern = re.compile("^[+\\-]")
        # _sort = list(sort)  # type list[str]
        _by = [x[1:] if pattern.match(x) else x for x in sort]
        _desc = [x.startswith('-') for x in sort]

        log.debug(type(sort))
        log.debug(sort)
        _ds = _df.sort_values(by=_by, ascending=_desc)
    else:
        _ds = _df
    return _ds.head(limit)
