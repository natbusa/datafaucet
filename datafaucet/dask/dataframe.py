def sample(df, n=1000, *col, seed=None):
    # n 0<float<=1 -> fraction of samples
    # n floor(int)>1 -> number of samples

    # todo:
    # n dict of key, value pairs or array of (key, value)
    # cols = takes alist of columns for sampling if more than one column is provided
    # if a stratum is not specified, provide equally with what is left over form the total of the other quota

    if n>1:
        count = df.count()
        fraction = n/count
        return df if fraction>1 else df.sample(False, fraction, seed=seed)
    else:
        return df.sample(False, n, seed=seed)
