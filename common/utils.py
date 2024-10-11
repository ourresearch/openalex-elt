from pyspark import F


def explode_dict_column(df, dict_column_name):
    """
    This utility function takes a DataFrame and a column name containing a dictionary (MapType),
    and explodes it into separate columns.

    :param df: The input DataFrame
    :param dict_column_name: The name of the column containing a dictionary (MapType)
    :return: A DataFrame with the dictionary keys exploded into separate columns
    """
    keys = [key for key in df.select(dict_column_name).first()[0].keys()]
    select_exprs = [F.col(dict_column_name)[key].alias(key) for key in keys]
    return df.select('*', *select_exprs).drop(dict_column_name)