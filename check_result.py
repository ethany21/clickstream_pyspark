from datetime import datetime


def get_month() -> str:
    if datetime.now().month < 10:
        return '0' + str(datetime.now().month)
    return str(datetime.now().month)


if __name__ == "__main__":
    print(get_month())
    # import pandas as pd
    #
    # df = pd.read_parquet("output-2023-02-22.parquet", engine='pyarrow')
    #
    # print(df)
