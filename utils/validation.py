def validate_no_null(df, column):
    if df[column].isnull().sum() > 0:
        raise ValueError(f"{column} contains NULL")


def validate_positive(df, column):
    if (df[column] < 0).any():
        raise ValueError(f"{column} contains negative values")