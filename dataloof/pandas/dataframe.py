import pandas as pd

def diff(df1, df2):
    """
      Find Difference of rows for given two dataframes
      this function is not symmetric, means
            diff(x, y) != diff(y, x)
    """
    if (df1.columns != df2.columns).any():
        raise ValueError("Two dataframe columns must match")

    if df1.equals(df2):
        return None
    else:
        return pd.concat([df1, df2, df2]).drop_duplicates(keep=False)